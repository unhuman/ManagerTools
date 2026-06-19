from enum import Enum


class WorkSource(Enum):
    """Where a sprint report sources its work.

    - PR:     pull requests (via Jira dev-status pull-request view). The historical default.
    - COMMIT: commits directly (via Jira dev-status commit view); no PR processing.
    - BOTH:   PRs plus any commits not already counted by a PR (de-duped by commit SHA).
    """
    PR = "pr"
    COMMIT = "commit"
    BOTH = "both"
