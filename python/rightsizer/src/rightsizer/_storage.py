"""Store options for Rust's object-store clients (SPEC §10.3).

Integrations resolve storage options for their own writes (Flyte's
object-store settings, fsspec ``storage_options``) and pass the same options
**explicitly** to the engine (``Engine(storage_options=...)`` →
``rightsizer._core.Engine(storage_options=...)``) and to
:class:`rightsizer_runtime.record.RunRecordWriter`. Nothing is written to
``os.environ``: exporting ``AWS_*`` would hand rightsizer's store identity to
the user's own boto3 / s3fs clients in the same process.

:func:`object_store_options` normalizes fsspec spellings (``key``,
``secret``, ``client_kwargs.endpoint_url``, …) to the ``object_store`` keys
Rust takes (``access_key_id``, ``secret_access_key``, ``endpoint``, …).
"""

from __future__ import annotations

from rightsizer_runtime.storage import flyte_storage_options, fsspec_options, object_store_options

__all__ = ["flyte_storage_options", "fsspec_options", "object_store_options"]
