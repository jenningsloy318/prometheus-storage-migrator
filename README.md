# prometheus-storage-migrator
To read local prometheus storage and write to remote storage

- parameters
  ```
  --read.storage.path: the path of the prometheus storage, which all metircs are stored on
  
  --write.remote.url: the remote write url, which the metrics will be stored to
  ```