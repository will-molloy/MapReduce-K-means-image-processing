#!/usr/bin/env stack
-- stack --install-ghc --resolver lts-6.23 runghc   --package transient transient-universe
{-# LANGUAGE  ExistentialQuantification, DeriveDataTypeable #-}

module Main where
import System.Environment (getArgs)
import Transient.Base
import Transient.Move
import Transient.Move.Utils
import Transient.MapReduce
import Control.Applicative
import Control.Monad.IO.Class
import Data.Monoid

import qualified Data.Map as M
import qualified Data.Foldable as F
import qualified Data.Vector as V

main :: IO ()
main = do
    args <- getArgs

    case args of
      nodeCount : file : [] -> do
        input <- readFile file
        let nodeN = read nodeCount :: Int
        runCloudIO $ do
            runTestNodes [2000..2000 + nodeN - 1]
            local $ option "start" "START!!!"
            r <- reduce (+) . mapKeyB (\w -> (w, 1 :: Int)) $ getText words input
            -- local $ exit (r :: M.Map String Int)
            lliftIO $ print (r :: M.Map String Int)
        return ()
      _ -> putStrLn "Usage: ./binary <NodeCount> <File>"

