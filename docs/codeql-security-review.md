# CodeQL security review

This review records the disposition of the critical and high-severity alert
categories reported after `security-extended` scanning was enabled. Alert
numbers are repository-local; the rule ID and data flow remain the durable
reference.

| Rule/category | Disposition | Control or evidence |
| --- | --- | --- |
| `py/partial-ssrf` in the AI client | Remediated | The web deployment defaults to the fixed OpenAI HTTPS origin. A custom destination requires an explicit server-side opt-in, private or special-purpose IP destinations require a second opt-in, and credentials, query strings, fragments, and non-loopback HTTP endpoints are rejected. |
| `py/weak-sensitive-data-hashing` in OnlyFans activation | Accepted external protocol | OnlyFans mandates SHA-1 for its rotating request-signature protocol. The digest is not used for OpenLedger authentication, password storage, evidence approval, or another security decision. Existing activation tests verify interoperability. |
| `py/path-injection` in investigation deletion | Remediated and regression-tested | Session folders must match a bounded ASCII allowlist. The deletion target is selected from a server-side directory enumeration, symlinks are refused, the resolved target must remain below the report root, and deletion uses a server-generated tombstone name. Tests prove traversal, separators, control characters, Unicode, and overlong values cannot affect an outside file. |
| `py/incomplete-url-substring-sanitization` in a checking test | Test-only false positive | The flagged expression asserts that an expected URL exists in parsed result data. It is not URL validation or a runtime security boundary. |
| `py/clear-text-logging-sensitive-data` in `create_auth.py` | False positive, regression-tested | The flagged message contains only the minimum-length constant. A subprocess regression test proves a rejected password is absent from both standard output and standard error. |

The same remediation also normalizes post-login redirect targets, removes raw
upstream exception details from browser events and persisted job errors, and
sanitizes untrusted log fields to a single bounded line. Client errors now carry
an opaque reference that can be correlated with the server log.

CodeQL now analyzes both Python and first-party JavaScript on pushes, pull
requests, and the weekly schedule. Any locally pinned third-party JavaScript in
the vendor directory is excluded from source analysis because it is reviewed
through its pinned version and dependency alerts rather than as authored
OpenLedger code.

After this change reaches `main`, rerun CodeQL. Close alerts whose data flow was
removed as fixed. Dismiss only the documented protocol and test-only findings,
using GitHub's specific dismissal reason and a link to this review; do not
dismiss an entire rule across the repository.
