import json
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

from .rest_service import RestService
from .auth_info import AuthInfo, AuthType


class GithubGraphQLClient(RestService):
    ENDPOINT = "https://api.github.com/graphql"

    _QUERY = """
    query GetPRData(
        $owner: String!, $repo: String!, $number: Int!,
        $commitCursor: String, $commentCursor: String, $reviewThreadCursor: String
    ) {
      rateLimit { cost remaining }
      repository(owner: $owner, name: $repo) {
        pullRequest(number: $number) {
          additions
          deletions
          createdAt
          mergedAt
          commits(first: 100, after: $commitCursor) {
            pageInfo { hasNextPage endCursor }
            nodes {
              commit {
                oid
                message
                additions
                deletions
                changedFilesIfAvailable
                committedDate
                author { name user { login } }
                committer { name user { login } }
              }
            }
          }
          comments(first: 100, after: $commentCursor) {
            pageInfo { hasNextPage endCursor }
            nodes {
              author { login }
              authorAssociation
              body
              createdAt
            }
          }
          reviewThreads(first: 50, after: $reviewThreadCursor) {
            pageInfo { hasNextPage endCursor }
            nodes {
              comments(first: 20) {
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

    def __init__(self, bearer_token: str):
        super().__init__(AuthInfo(AuthType.Bearer, bearer_token))

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

        while True:
            variables = {
                "owner": owner,
                "repo": repo,
                "number": pr_number,
                "commitCursor": None if commits_done else commit_cursor,
                "commentCursor": None if comments_done else comment_cursor,
                "reviewThreadCursor": None if rt_done else rt_cursor,
            }

            body = json.dumps({"query": self._QUERY, "variables": variables})
            response = self._execute_request("POST", self.ENDPOINT, body, {})

            if "errors" in response:
                raise RuntimeError(f"GraphQL errors for {owner}/{repo}#{pr_number}: {response['errors']}")

            data = response.get("data", {})
            rate_limit = data.get("rateLimit", {})
            print(f"      [DEBUG] GraphQL: cost={rate_limit.get('cost', '?')}, remaining={rate_limit.get('remaining', '?')}")

            pr = data.get("repository", {}).get("pullRequest") or {}

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
                    all_commits.append(node.get("commit", {}))
                page_info = commits_page.get("pageInfo", {})
                commits_done = not page_info.get("hasNextPage", False)
                if not commits_done:
                    commit_cursor = page_info.get("endCursor")

            if not comments_done:
                comments_page = pr.get("comments", {})
                for node in comments_page.get("nodes", []):
                    all_comments.append(node)
                page_info = comments_page.get("pageInfo", {})
                comments_done = not page_info.get("hasNextPage", False)
                if not comments_done:
                    comment_cursor = page_info.get("endCursor")

            if not rt_done:
                rt_page = pr.get("reviewThreads", {})
                for thread in rt_page.get("nodes", []):
                    for comment in thread.get("comments", {}).get("nodes", []):
                        all_rt_comments.append(comment)
                page_info = rt_page.get("pageInfo", {})
                rt_done = not page_info.get("hasNextPage", False)
                if not rt_done:
                    rt_cursor = page_info.get("endCursor")

            if commits_done and comments_done and rt_done:
                break

        return {
            "pr": pr_meta,
            "commits": all_commits,
            "comments": all_comments,
            "review_thread_comments": all_rt_comments,
        }
