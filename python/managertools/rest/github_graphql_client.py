import json
import sys
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

from .rest_service import RestService
from .auth_info import AuthInfo, AuthType
from .exceptions import RESTException
from ..util.log_util import debug_print


class GithubGraphQLClient(RestService):
    ENDPOINT = "https://api.github.com/graphql"
    _max_transient_retries = 0  # 502s handled via commit page-size reduction below
    # Fresh pulls start large and step down on 502 (floor at 1). Re-running an
    # incomplete cache's previously-failed tickets uses the small ladder, since
    # those are the known-problematic large PRs that already 502'd at large sizes.
    _COMMIT_PAGE_SIZES_INITIAL = [20, 10, 5, 2, 1]
    _COMMIT_PAGE_SIZES_RETRY = [2, 1]
    _page_size_reductions: Dict[str, int] = {}  # pr_key -> page size that succeeded

    _QUERY = """
    query GetPRData(
        $owner: String!, $repo: String!, $number: Int!,
        $commitPageSize: Int!,
        $commitCursor: String, $commentCursor: String, $reviewThreadCursor: String
    ) {
      rateLimit { cost remaining limit resetAt }
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $number) {
          additions
          deletions
          createdAt
          mergedAt
          commits(first: $commitPageSize, after: $commitCursor) {
            totalCount
            pageInfo { hasNextPage endCursor }
            nodes {
              commit {
                oid
                message
                additions
                deletions
                changedFilesIfAvailable
                committedDate
                parents { totalCount }
                author { name user { login } }
                committer { name user { login } }
              }
            }
          }
          comments(first: 100, after: $commentCursor) {
            totalCount
            pageInfo { hasNextPage endCursor }
            nodes {
              author { login }
              authorAssociation
              body
              createdAt
            }
          }
          reviewThreads(first: 50, after: $reviewThreadCursor) {
            totalCount
            pageInfo { hasNextPage endCursor }
            nodes {
              comments(first: 100) {
                nodes {
                  author { login }
                  authorAssociation
                  body
                  createdAt
                }
              }
            }
          }
          reviews(first: 100) {
            nodes {
              author { login }
              authorAssociation
              body
              state
              submittedAt
            }
          }
        }
      }
    }
    """

    def __init__(self, bearer_token: str, graphql_points_reserved: int = 5):
        super().__init__(AuthInfo(AuthType.Bearer, bearer_token))
        self._graphql_points_reserved = graphql_points_reserved
        self._pr_progress_index: int = 0
        self._pr_progress_total: int = 0
        self._commit_page_sizes: List[int] = self._COMMIT_PAGE_SIZES_INITIAL

    def set_pr_progress(self, index: int, total: int) -> None:
        """Set current PR progress for debug logging."""
        self._pr_progress_index = index
        self._pr_progress_total = total

    def set_retry_mode(self, enabled: bool) -> None:
        """Use the small commit page-size ladder when re-fetching known-failed tickets."""
        self._commit_page_sizes = (self._COMMIT_PAGE_SIZES_RETRY if enabled
                                   else self._COMMIT_PAGE_SIZES_INITIAL)

    def get_pull_request_data(self, owner: str, repo: str, pr_number: int) -> Dict[str, Any]:
        """
        Fetch all PR data via paginated GraphQL queries.
        Returns raw GraphQL data shaped for GithubREST.get_pull_request_full() to normalize.
        """
        all_commits: List[Dict] = []
        all_comments: List[Dict] = []
        all_rt_comments: List[Dict] = []
        pr_meta: Optional[Dict] = None

        commits_done = False
        comments_done = False
        rt_done = False
        commit_cursor: Optional[str] = None
        comment_cursor: Optional[str] = None
        rt_cursor: Optional[str] = None
        commit_page_size = self._commit_page_sizes[0]
        commit_page_size_idx = 0

        # For progress tracking
        import math
        call_num = 0
        commits_total: Optional[int] = None
        comments_total: Optional[int] = None
        rt_total: Optional[int] = None

        while True:
            variables = {
                "owner": owner,
                "repo": repo,
                "number": pr_number,
                "commitPageSize": commit_page_size,
                "commitCursor": None if commits_done else commit_cursor,
                "commentCursor": None if comments_done else comment_cursor,
                "reviewThreadCursor": None if rt_done else rt_cursor,
            }

            body = json.dumps({"query": self._QUERY, "variables": variables})
            try:
                response = self._execute_request("POST", self.ENDPOINT, body, {})
            except RESTException as e:
                if e.status_code == 502 and commit_page_size_idx < len(self._commit_page_sizes) - 1:
                    commit_page_size_idx += 1
                    commit_page_size = self._commit_page_sizes[commit_page_size_idx]
                    sys.stderr.write(f"\033[91m502 fetching {owner}/{repo}#{pr_number}, reducing commit page size to {commit_page_size}\033[0m\n")
                    continue
                raise

            if "errors" in response:
                if response.get("data") is None:
                    raise RuntimeError(f"GraphQL errors for {owner}/{repo}#{pr_number}: {response['errors']}")
                sys.stderr.write(
                    f"GraphQL partial errors for {owner}/{repo}#{pr_number} "
                    f"(continuing with available data): {response['errors']}\n"
                )

            data = response.get("data", {})
            rate_limit = data.get("rateLimit", {})
            remaining = rate_limit.get('remaining')
            reset_at_str = rate_limit.get('resetAt')
            reset_dt = datetime.fromisoformat(reset_at_str.replace('Z', '+00:00')) if reset_at_str else None
            seconds_until_reset = max(0, int(reset_dt.timestamp() - time.time())) if reset_dt else None

            pr = data.get("repository", {}).get("pullRequest") or {}

            # Track call number and capture total counts for progress estimation
            call_num += 1
            if commits_total is None:
                commits_total = pr.get("commits", {}).get("totalCount")
            if comments_total is None:
                comments_total = pr.get("comments", {}).get("totalCount")
            if rt_total is None:
                rt_total = pr.get("reviewThreads", {}).get("totalCount")

            # Estimate total pages needed for this PR
            est = max(
                math.ceil((commits_total or 0) / commit_page_size) if commits_total else 1,
                math.ceil((comments_total or 0) / 100) if comments_total else 1,
                math.ceil((rt_total or 0) / 50) if rt_total else 1,
                1
            )

            # Log progress
            pr_prog = f"PR {self._pr_progress_index}/{self._pr_progress_total}" if self._pr_progress_total else "PR ?/?"
            debug_print(
                f"GraphQL ({pr_prog}, call {call_num}/~{est}): cost={rate_limit.get('cost', '?')}, "
                f"remaining={remaining}/{rate_limit.get('limit', '?')}, "
                f"reset_in={seconds_until_reset}s"
            )

            if pr_meta is None:
                pr_meta = {
                    "additions": pr.get("additions", 0),
                    "deletions": pr.get("deletions", 0),
                    "createdAt": pr.get("createdAt"),
                    "mergedAt": pr.get("mergedAt"),
                    "reviews": pr.get("reviews", {}).get("nodes", []),
                }

            if not commits_done:
                commits_page = pr.get("commits", {})
                for node in commits_page.get("nodes", []):
                    if node is not None:
                        commit = node.get("commit")
                        if commit is not None:
                            all_commits.append(commit)
                page_info = commits_page.get("pageInfo", {})
                commits_done = not page_info.get("hasNextPage", False)
                if not commits_done:
                    commit_cursor = page_info.get("endCursor")

            if not comments_done:
                comments_page = pr.get("comments", {})
                for node in comments_page.get("nodes", []):
                    if node is not None:
                        all_comments.append(node)
                page_info = comments_page.get("pageInfo", {})
                comments_done = not page_info.get("hasNextPage", False)
                if not comments_done:
                    comment_cursor = page_info.get("endCursor")

            if not rt_done:
                rt_page = pr.get("reviewThreads", {})
                for thread in rt_page.get("nodes", []):
                    if thread is None:
                        continue
                    for comment in thread.get("comments", {}).get("nodes", []):
                        if comment is not None:
                            all_rt_comments.append(comment)
                page_info = rt_page.get("pageInfo", {})
                rt_done = not page_info.get("hasNextPage", False)
                if not rt_done:
                    rt_cursor = page_info.get("endCursor")

            if commits_done and comments_done and rt_done:
                break

            if (remaining is not None
                    and remaining <= self._graphql_points_reserved
                    and seconds_until_reset is not None):
                sys.stderr.write(
                    f"GraphQL remaining={remaining} ≤ {self._graphql_points_reserved} (graphqlPointsReserved), "
                    f"pausing {seconds_until_reset + 2}s until reset\n"
                )
                self._wait_with_countdown(seconds_until_reset + 2, "Proactive rate limit")

        if commit_page_size != self._commit_page_sizes[0]:
            pr_key = f"{owner}/{repo}#{pr_number}"
            GithubGraphQLClient._page_size_reductions[pr_key] = commit_page_size
            sys.stderr.write(
                f"Commit page size reduction summary: {owner}/{repo}#{pr_number} "
                f"required page size {commit_page_size} "
                f"(default {self._commit_page_sizes[0]}). "
                f"All reductions this run: {GithubGraphQLClient._page_size_reductions}\n"
            )

        return {
            "pr": pr_meta,
            "commits": all_commits,
            "comments": all_comments,
            "review_thread_comments": all_rt_comments,
        }
