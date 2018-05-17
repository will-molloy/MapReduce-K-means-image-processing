{-# LANGUAGE TupleSections, DeriveGeneric, OverloadedStrings #-}
module Reddit
  ( Document
  , distMR
  , __remoteTable
  ) where

import Control.Distributed.Process
import Control.Distributed.Process.Closure
import MapReduce 
import MonoDistrMapReduce hiding (__remoteTable) 
import Data.Aeson
import GHC.Generics
import qualified Data.String as S

data RedditJsonObj = RedditJsonObj { subreddit :: String, score :: Int } deriving (Generic, Show)
instance FromJSON RedditJsonObj where

type Document  = String
type Word      = String
type Score = Int
type Count = Int
type Frequency = Int

redditMR :: MapReduce FilePath Document String Score Score
redditMR = MapReduce {
    mrMap    = mrMap'
  , mrReduce = mrReduce'
}

mrReduce' k vs = (sum vs) `div` (length vs)
mrMap' k v = helper <$> ((decode . S.fromString) <$> lines v)
             where helper (Just obj) = (subreddit obj, score obj)
                   helper Nothing = ("", 0)

redditMR_ :: () ->  MapReduce FilePath Document String Score Score
redditMR_ () = redditMR

remotable ['redditMR_]

distMR :: [NodeId] -> Map FilePath Document -> Process (Map String Score)
distMR = distrMapReduce ($(mkClosure 'redditMR_) ()) 
