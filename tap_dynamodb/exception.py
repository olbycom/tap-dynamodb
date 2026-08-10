"""Custom exceptions."""


class EmptyTableException(Exception):
    """Raised when the DyanmoDB table is empty."""

    pass


class QueryAccessDeniedException(Exception):
    """Raised when a Query is denied by IAM, so the caller can fall back to Scan.

    Querying a GSI needs `dynamodb:Query` on the index ARN (`table/<name>/index/<index>`),
    which is a distinct resource from the table ARN -- a policy granting Query on the table
    alone denies this. Surfaced separately from other ClientErrors because it degrades
    cleanly to a Scan instead of having to fail the run.
    """

    pass
