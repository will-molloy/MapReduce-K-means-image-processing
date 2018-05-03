import Transient.Move (runCloudIO, lliftIO, createNode, connect, getNodes, onAll)

main = runCloudIO $ do
    this   <- lliftIO (createNode lh 8000)
    master <- lliftIO (createNode lh 8001)
    connect this master
    onAll getNodes >>= lliftIO . putStrLn . show
    where lh = "0.0.0.0"
