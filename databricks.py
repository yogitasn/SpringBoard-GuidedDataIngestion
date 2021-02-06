storageAccountName = "storageyogid"
storageAccountAccessKey = "/NCFnqJlD0ZcOAgNEf1tr3ff3BXlEgHJ85Xx/QAUsg9nxnXv/tRSK5WgDyjJXhaIBWOndqTMsapk+U02QVxO2w=="
blobContainerName = "guidedstorage"


if not any(mount.mountPoint == '/mnt/FileStore/MountFolder/' for mount in dbutils.fs.mounts()):
  try:
    dbutils.fs.mount(
    source = "wasbs://{}@{}.blob.core.windows.net".format(blobContainerName, storageAccountName),
    mount_point = "/mnt/FileStore/MountFolder/",
    extra_configs = {'fs.azure.account.key.' + storageAccountName + '.blob.core.windows.net': storageAccountAccessKey}
  )
  except Exception as e:
    print("already mounted. Try to unmount first")

display(dbutils.fs.ls("dbfs:/mnt/FileStore/MountFolder/data"))