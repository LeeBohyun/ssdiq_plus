library(ggplot2)
library(sqldf)
library(Cairo)
library(gridExtra)
library(reshape)
library(scales)
'%+%' <- function(x, y) paste0(x,y)
# Greedy Approx 2 alpha
greedyApproxFree = function(fillLevel) { 
	free = 2*(1-fillLevel)
}
greedyApproxWA = function(fillLevel) { 1/greedyApproxFree(fillLevel)}

# Greddy Approx
greedyApproxFree = function(fillLevel) { 
	free = 0.982287 + 0.2632311*fillLevel - 0.682966*fillLevel^2 - 2.221004*fillLevel^3 + 2.548378*fillLevel^4 - 0.8903583*fillLevel^5
	free = min(free, 0.99999999999999)
	free = max(free, 0.00000000000001)
	free
}
greedyApproxWA = function(fillLevel) { 1/greedyApproxFree(fillLevel)}
1/greedyApproxFree(0.9)


calc2aSolution = function(w) {
	x = (w - sqrt(-((-1 + w) * w)))/(-1 + 2 * w) 
	#x = (w + sqrt(-((-1 + w) * w)))/(-1 + 2 * w)
	x
}
formula_a_claude <- function(x) {
  numerator <- prod(x)
  denominator <- 1 + sum(x) + sum(combn(x, 2, prod)) + prod(x)
  return(numerator / denominator)
}
formula_a_claude_2 <- function(x) {
  n <- length(x)
  numerator <- prod(x)
  denominator <- 1
  for (i in 1:n) {
    denominator <- denominator * (1 + sum(x[-i]))
  }
  return(numerator / denominator)
}
formula_a_claude_3 <- function(x) {
  n <- length(x)
  numerator <- prod(x)
  denominator <- 1
  for (i in 1:n) {
    denominator <- denominator * (1 + sum(x[-(1:i)]))
  }
  return(numerator / denominator)
}
formula_a_claude_4 <- function(x) {
  n <- length(x)
  numerator <- prod(x)
  denominator <- 1
  for (i in 1:n) {
    denominator <- denominator * (1 + sum(x[i:n]))
  }
  return(numerator / denominator)
}
compute_a_gpt4 <- function(x_vec) {
  prod_x = cumprod(x_vec)
  denom = sum(c(1, prod_x))
  return(prod_x[length(x_vec)] / denom)
}
compute_a_gpt4_2 <- function(x_vec) {
  l = length(x_vec)
  a = prod(x_vec) / (1 + sum(sapply(l:(l-1), function(k) prod(x_vec[(l-k+1):l]))))
  return(a)
}
compute_a_gpt4_3 <- function(x_vec) {
  n = length(x_vec)
  x1 = x_vec[1]
  if (n == 1) {
    a = x1
  } else {
    x2_x_n = x_vec[2:n]  # from x2 to xn
    patial_prod = cumprod(x2_x_n)
    denom = 1 + sum(patial_prod)
    a = (x1 * patial_prod[n - 1]) / denom
  }
  return(a)
}
compute_a_gpt4_4 <- function(x_vec) {
  num = prod(x_vec)
  denom = 1
  for (i in 1:length(x_vec)) {
    denom = denom + prod(x_vec[i:length(x_vec)])
  }
  return(num / denom)
}
compute_a = function (x_vex) {
	return(compute_a_gpt4_4(x_vex))
}
newOptWA = function(fillLevel, s_rel, wf_rel) {
	wf = wf_rel/sum(wf_rel)
	wf_cnt = length(wf_rel)
	id = seq(1, wf_cnt)
	wfd = data.frame(id, wf)
	# ignore sizes
	wfd = sqldf("select *, wf / (wf + LAG(wf, -1) over (order by id)) rel from wfd")
	wfd$splits = calc2aSolution(wfd$rel)
	wfd$factors = wfd$splits / (1 - wfd$splits)
	factors <- wfd$factors[!is.na(wfd$factors)]
	firsta = c(compute_a(factors))
	for (i in 1:length(factors)) {
		firsta = c(firsta, firsta[i]/factors[i])
	}
	return(firsta)
}
newOptWA(0.9, s_rel=c(1, 1, 1, 1), wf_rel=c(0.70, 0.15, 0.1, 0.05))
calcOptWA = function(fillLevel, s_rel, wf_rel) {
	wf = wf_rel/sum(wf_rel)
	s = s_rel/sum(s_rel)*fillLevel
	s_cnt = length(s_rel)
	stopifnot(s_cnt == length(wf_rel))
	# generate combinations
	interval_diff = 0.001
	interval_max = 1
	split_cnt = s_cnt -1
	splitsToIntervals = function(interval) {
		intervalExt = c(0, interval, 1)
		cc = c()
		for (idx in 1:(split_cnt+1)) {
			cc = c(cc, intervalExt[idx+1] - intervalExt[idx] )
		}
		cc
	}
	intervalWA = function(opInterval) {
		sumWA = 0
		#cat("opInterval: ")
		#print(opInterval)
		for (i in 1:s_cnt) {
			#cat("s: ")
			#cat(s)
			#print("")
			sFillLevel = s[i] / (s[i] + (1-fillLevel)*opInterval[i])
			#cat(i %+% " sfill: " %+% round(sFillLevel, 2))
			#cat(" -> " %+% round(greedyApproxWA(sFillLevel), 2) %+% ", ")
			sumWA = sumWA + wf[i] * greedyApproxWA(sFillLevel)
		}
		#print("")
		#print("wa: " %+% round(sumWA, 2))
		sumWA
	}
	intervals = matrix(nrow=(1/interval_diff)^split_cnt, ncol=split_cnt+2)
	lastSplit = seq(interval_diff, interval_diff*split_cnt, interval_diff)
	actual_interval = splitsToIntervals(lastSplit)
	intervals[1,] = c(actual_interval, intervalWA(actual_interval))
	interval_cnt = 1
	# todo remove unnecessary splits matrix.
	# first splits are generated i.e. where the interval is split between [0,1] like (0, 0.5, 0.8, 1)
	# the differences between are then used to generate interval sizes i.e. (0.5-0, 0.8-0.5, 1-0.8) = (0.5, 0.3, 0.2) => sum is always 1
	while(1) {
		if (interval_cnt %% 100000 == 0) {
			print("interval_cnt: " %+% interval_cnt %+% " last: " %+% lastSplit)
		}
		interval = lastSplit
		for (j in split_cnt:1) {
			if (interval[j]+interval_diff < interval_max - interval_diff*(split_cnt-j)) {
				interval[j] = interval[j]+interval_diff
				if (j < split_cnt) {
					for (k in seq(j+1, split_cnt)) {
						interval[k] = interval[k-1]+interval_diff
					}
				}
				break
			}
		}
		lastSplit = interval
		# calculate average WA for all intervals
		# avgWA = wf0*greedyWA(s0, op0) + ....
		# the intervals are the key to how op is distributed.
		actual_interval = splitsToIntervals(interval)
		intervals[interval_cnt+1,] = c(actual_interval, intervalWA(actual_interval))
		if (interval_cnt > 11200) { 
			#print(interval)
			#print(" diff: " %+% (interval[1]+interval_diff) %+% " > " %+% (interval_max - interval_diff*(split_cnt-1)) )
		}
		if (interval[1]+interval_diff > interval_max - interval_diff*(split_cnt-1)) {
			break
		}
		interval_cnt = interval_cnt+1 
	}
	#print("done: " %+% interval_cnt)
	intervals = data.frame(intervals[1:(interval_cnt+1),])
	colnames(intervals)[ncol(intervals)] <- "WA"
	#print(sqldf("select *from intervals"))
	intervals =  sqldf("select *, row_number() over (order by WA) as rank from intervals")
	sqldf("select *, 1/WA from intervals where rank <= 1")
}
calcOptWA2 = function(fillLevel, s_rel, wf_rel) {
	wf = wf_rel/sum(wf_rel)
	s = s_rel/sum(s_rel)*fillLevel
	s_cnt = length(s_rel)
	intervalWA = function(opInterval) {
		sumWA = 0
		for (i in 1:s_cnt) {
			sFillLevel = s[i] / (s[i] + (1-fillLevel)*opInterval[i])
			sumWA = sumWA + wf[i] * greedyApproxWA(sFillLevel)
		}
		# penalty if sum of opInterval is != 1 
		if(abs(1 - sum(opInterval)) > 1e-5) {
			sumWA = sumWA + 1e5 * (1 - sum(opInterval))^2
		}
		return(sumWA)
	}
	print(s)
	result <- optim(rep(1/s_cnt, s_cnt), intervalWA, 
					gr = NULL, 
					method = "L-BFGS-B", # bounded optimization
                lower = rep(0, s_cnt), upper = rep(1, s_cnt))
	cat(result$par)
	cat(" WA: ")
	cat(result$value)
}
print("wa")
