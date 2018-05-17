import System.Environment (getArgs)
import System.IO
import Control.Applicative
import Control.Monad
import System.Random
import Control.Distributed.Process
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Data.Map (Map)
import Data.Array (Array, listArray)
import qualified Data.Map as Map (fromList)

import qualified Reddit
import qualified MonoDistrMapReduce

rtable :: RemoteTable
rtable = MonoDistrMapReduce.__remoteTable 
       . Reddit.__remoteTable
       $ initRemoteTable 

main :: IO ()
main = do
  args <- getArgs

  case args of
    -- Distributed word count
    "master" : host : port : "reddit" : files -> do
      input   <- constructInput files 
      backend <- initializeBackend host port rtable 
      startMaster backend $ \slaves -> do
        result <- Reddit.distMR slaves input 
        liftIO $ print result 

    -- Generic slave for distributed examples
    "slave" : host : port : [] -> do
      backend <- initializeBackend host port rtable 
      startSlave backend

--------------------------------------------------------------------------------
-- Auxiliary                                                                  --
--------------------------------------------------------------------------------

constructInput :: [FilePath] -> IO (Map FilePath Reddit.Document)
constructInput files = do
  contents <- mapM readFile files
  return . Map.fromList $ zip files contents

arrayFromList :: [e] -> Array Int e
arrayFromList xs = listArray (0, length xs - 1) xs
