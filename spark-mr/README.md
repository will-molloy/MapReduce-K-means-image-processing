# How to build

You need `etlas` from `eta-lang`

run `etlas build`

## How to run is on spark

upon building, find the executable jar and run
`/$SPARK_HOME/bin/spark-submit --class eta.main --master spark://0.0.0.0:7077 --jars spark-eta-example.jar,/home/e/.etlas/lib/eta-0.7.1.3/base-4.8.2.0-IPFDtZjmEfh5tPXF10v5hF/base-4.8.2.0-IPFDtZjmEfh5tPXF10v5hF.jar,/home/e/.etlas/store/eta-0.7.1.3/eta-scala-interop-0.1.0.0-14601e4473c093390a62e9afa8c3b8634c457616b79a7fec10876ff2c21d4a4d/lib/eta-scala-interop-0.1.0.0-14601e4473c093390a62e9afa8c3b8634c457616b79a7fec10876ff2c21d4a4d.jar,/home/e/.etlas/store/eta-0.7.1.3/eta-spark-core-0.1.1.0-0d8b6d80e92d3240da4141f96f2e7281c8d1c42cf21fdb9c00088e87bfc49a56/lib/eta-spark-core-0.1.1.0-0d8b6d80e92d3240da4141f96f2e7281c8d1c42cf21fdb9c00088e87bfc49a56.jar,/home/e/.etlas/lib/eta-0.7.1.3/ghc-prim-0.4.0.0-Jhi6UgHuZdoBZWUpVo3WKE/ghc-prim-0.4.0.0-Jhi6UgHuZdoBZWUpVo3WKE.jar,/home/e/.etlas/lib/eta-0.7.1.3/integer-0.5.1.0-ACyqTmAMUMGGvisLFeQbAm/integer-0.5.1.0-ACyqTmAMUMGGvisLFeQbAm.jar,/home/e/.etlas/lib/eta-0.7.1.3/rts-0.1.0.0-5Dj4qf1Wx9cCeYbpni5T1w/rts-0.1.0.0-5Dj4qf1Wx9cCeYbpni5T1w.jar spark-mr.jar`

Note the `--jars` argument is from `etlas deps`, the exact run command might be different
