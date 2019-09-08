#!/bin/bash

mkdir -p outputs

for type in 'RANGE' 'JOIN'; do
	for attr in 'o_orderkey' 'o_totalprice' 'l_orderkey' 'l_extendedprice'; do
		for outvar in 'Avg_Execution_Time' 'Result_Set_Returned'; do
			echo "Running experiment for predictions of ${outvar} of ${type} queries with range constraint on ${attr}..."
			/usr/bin/time -o outputs/r.time-${type}-${attr}-${outvar} \
				python3 regression.py  -t ${type} -i ${attr} -o ${outvar} \
				2>>outputs/r.err-${type}-${attr}-${outvar} | \
				tee -a outputs/r.out-${type}-${attr}-${outvar};
		done
	done
done
