# MapReduce on Distributed-process

Credit goes to http://www.well-typed.com/blog/73/

# How To build

You must have the haskell stack build tool, run `which cabal` to find out if you have it
run `cabal install --only-dependencies`
then
run `cabal build`

# Usage

## To Start Master
/path/to/binary master <host> <port> reddit <inputfile>

## To Start Slave
/path/to/binary slave <host> <port>

Run the slave first then run the master!

example result:

fromList
[("AdviceAnimals",1),("Android",0),("AskReddit",1),("Autos",1),("BBW",1),("BMW",1),("BabyBumps",2),("BostonBruins",1),("CanadaPolitics",3),("Cardinals",5),("Celebs",2),("DestinyTheGame",3),("EliteDangerous",1),("EverythingScience",10),("GCXRep",1),("Games",1),("GrandTheftAutoV",2),("HannibalTV",1),("HistoricalPowers",1),("Insurance",2),("LSD",3),("MLPLounge",2),("MLS",41),("MakeupAddiction",1),("Metroid",3),("Mustang",2),("NSFW_GIF",19),("Naruto",2),("Portland",1),("PuzzleAndDragons",1),("RandomActsOfSTC",2),("Reformed",2),("Seattle",2),("Smite",1),("Stacked",4),("TrollXChromosomes",18),("TumblrInAction",2),("WTF",6),("Wishlist",3),("badkarma",3),("beercanada",1),("beertrade",1),("casualiama",1),("changemyview",17),("childfree",2),("curvy",1),("dayz",1),("dbz",1),("devils",3),("elliottsmith",5),("exmormon",14),("explainlikeimfive",1),("falcons",3),("freedonuts",2),("funny",3),("guitarpedals",2),("gwcumsluts",2),("headphones",1),("hiphopheads",2),("hockey",5),("knives",1),("leagueoflegends",1),("mildlyinfuriating",2),("movies",1),("nba",1),("needadvice",1),("news",-1),("nfl",-1),("pics",1),("playrust",1),("pokemon",1),("pokemontrades",1),("randomsuperpowers",1),("roosterteeth",4),("sausagetalk",2),("summonerschool",1),("techsupport",1),("thelastofus",1),("tifu",1),("todayilearned",14),("twinks",2),("videos",5),("worldnews",0),("wow",1)]

