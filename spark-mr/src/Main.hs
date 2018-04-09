{-# LANGUAGE RankNTypes, ScopedTypeVariables, TypeOperators, FlexibleContexts, MultiParamTypeClasses, FlexibleInstances #-}

module Main where

import Java
import Java.Collections
import qualified Java.String as JS
import qualified Spark.Core as S
import Control.Applicative
import Control.Monad
import Data.Monoid
import System.Random hiding (split)
import Interop.Scala

foreign import java unsafe split :: JString -> Java a JStringArray

data Point = Point Double Double

instance Monoid Point where
    mappend (Point a b) (Point c d) = Point (a+c) (b+d)
    mempty = Point 0 0

instance JavaConverter Point (S.Tuple2 JDouble JDouble) where
    toJava (Point a b) = toJava (toJava a ::JDouble, toJava b :: JDouble )
    fromJava tuple = Point (fromJava a :: Double) (fromJava b :: Double)
                     where (a, b) = fromJava tuple :: (JDouble, JDouble)


type Tuple2Double = S.Tuple2 JDouble JDouble
type MappedKVT2Double = S.Tuple2 (Tuple2Double) JInteger

instance JavaConverter (Point, Integer) MappedKVT2Double where
    toJava (pt, i) = toJava $ (toJava pt :: Tuple2Double, toJava i :: JInteger)
    fromJava tuple = (fromJava a :: Point, fromJava b :: Integer) 
                     where (a, b) = fromJava tuple :: (Tuple2Double, JInteger)

unwrapTuple2Double :: MappedKVT2Double -> (Point, Integer)
unwrapTuple2Double v = (pt, int)
                        where (wrappedT, wrappedI) = fromJava v :: (S.Tuple2 JDouble JDouble, JInteger)
                              int = fromJava wrappedI :: Integer
                              pt = fromJava wrappedT :: Point


data JRandom = JRandom @java.lang.Random
    deriving Class

foreign import java unsafe "java.lang.Random.nextDouble" jnextDouble :: JRandom -> Java a JDouble
foreign import java unsafe "@new" newRandom :: Java a JRandom

checkLetter :: Char -> (forall a. JString -> Java a JBoolean)
checkLetter c str = return $ toJava (c `elem` str')
  where str' = fromJava str :: String

liftJava :: forall a b c. (a -> c) -> a -> Java b c
liftJava f arg = return $ f arg


main :: IO ()
main = java $ do
  conf <- S.newSparkConf
  conf <.> S.setAppName "MapReduce Kmeans"
  sc <- S.newSparkContext conf
  initialCents <- io $ randomCents count
  file <- sc <.> S.textFile inputFile >- S.cache
  points <- file <.> S.map toPts
  cluster <- points <.> S.mapToPair (liftJava $ findClosest' initialCents) 
  jmap <- cluster <.> S.reduceByKeyLocallyPair reducer
  --let clusters = toJava $ jMapMap jmap :: Map Tuple2Double Tuple2Double
  let clusters = jMapMap jmap
  io $ putStrLn $ show clusters
  
  io $ putStrLn "HEWWO???"
  sc <.> S.stop
  where inputFile = "test.data"
        count = 4

jMapMap :: Map Tuple2Double MappedKVT2Double -> [(Tuple2Double, Tuple2Double)]
jMapMap m = fmap (\(a, kvpair) -> (a, avgPts kvpair)) cmap
              where cmap = fromJava m :: [(Tuple2Double, MappedKVT2Double)]

avgPts :: MappedKVT2Double -> Tuple2Double
avgPts mapped = toJava $ Point (a / i) (b / i)
                where ((Point a b), v) = fromJava mapped :: (Point, Integer)
                      i = fromIntegral v



reducer :: forall a. MappedKVT2Double -> MappedKVT2Double -> Java a (Tuple2 (Tuple2 JDouble JDouble) JInteger)
reducer a b = return $ (toJava (toJava $ o <> p :: Tuple2Double, toJava $ k+n :: JInteger) :: (Tuple2 (Tuple2 JDouble JDouble) JInteger))
                  where (o@(Point i j), k) = unwrapTuple2Double a
                        (p@(Point l m), n) = unwrapTuple2Double b
                
            

randomCents :: Int -> IO [Point]
randomCents n = do
    let l = take n [1..]
    forM l (\x -> do
        a <- randomIO
        b <- randomIO
        return $ Point a b)

toPts :: JString -> Java a (Tuple2 JDouble JDouble)
toPts str = do 
  splitted <- split str
  a <- splitted <.> aget 0
  b <- splitted <.> aget 1
  return $ toJava $ Point (read $ fromJava a :: Double) (read $ fromJava b :: Double)

-- Find the nearest centeroid of point
findClosest :: [Point] -> S.Tuple2 JDouble JDouble -> S.Tuple2 JDouble JDouble
findClosest all@(x:xs) ptd = 
  toJava $ fst $ foldr ff (pt, ptDist x pt) xs
    where ff newPt (currentPt, currentDist) = if newDist < currentDist then (newPt, newDist) else (currentPt, currentDist) where newDist = ptDist newPt pt
          pt = fromJava ptd

findClosest' :: [Point] -> S.Tuple2 JDouble JDouble -> Tuple2 Tuple2Double (Tuple2 (Tuple2 JDouble JDouble) JInteger)
findClosest' all@(x:xs) ptd = 
  (toJava (ptd, toJava $ (fst $ foldr ff (pt, ptDist x pt) xs, 1 :: Integer) :: MappedKVT2Double)) :: Tuple2 Tuple2Double (Tuple2 (S.Tuple2 JDouble JDouble) JInteger)
    where ff newPt (currentPt, currentDist) = if newDist < currentDist then (newPt, newDist) else (currentPt, currentDist) where newDist = ptDist newPt pt
          pt = fromJava ptd

--findClosest' :: JArray [Point] Point -> Point -> Point
--findClosest' pts pt = do 
    --all <- pts
    --return $ (,) <$> findClosest all pt <*> 1
ptDist :: Point -> Point -> Double
ptDist (Point a b) (Point c d) = sqrt $ (c - a) ** 2 + (d - b) ** 2


