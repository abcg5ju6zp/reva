Bugfix: Prevent duplicate rclone jobs and make transfer final states authoritative

We've fixed a race in the rclone data transfer driver where a directory
migration could be accepted by the rclone daemon and then fail to be stored
locally. The user received a creation error while the remote job kept
copying, and submitting the request again could start a second job.

Transfer creation now uses a stable, credential-independent identity that
associates the CS3 transfer record, the remote job mapping, both endpoint
credentials and a queryable status. Success is confirmed only once the local
information is sufficient to take over the remote result. Retries of the
same request adopt the existing transfer instead of starting another job.
If the remote start response is lost, the daemon is queried by identity; if
the local mapping cannot be published, the unconfirmed job is cancelled and
a failure record is retained.

Cancel and completion can no longer interleave into contradictory states:
each remote (re)start bumps a transfer generation, late status updates of
an older remote job are discarded, and within one generation the first
observed final state is unique. If a final status cannot be persisted, the
monitor keeps converging the observed result, and a service restart takes
over the original job.
