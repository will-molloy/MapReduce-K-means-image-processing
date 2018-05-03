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

main = keep $ do
         initNode $ inputNodes <|> (local $ return "0")
         return ()
