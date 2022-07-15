library(ggplot2)
library(sqldf)
library(metR)
library(tikzDevice)
library(viridis)
library(ggforce)

tikzheight <- 1.25
tikzwidth <- 1.8



throughput_data <- read.csv("../benchmarks/throughput_variable_sized.csv")
varsize_data <- read.csv("../benchmarks/throughput_varying_payload.csv")

throughput_data$datastructure <- factor(throughput_data$datastructure, 
                                        levels=c("PLUSH","Dash","FASTER","PMEMKV","rocksdb-pmem","Viper"))

varsize_data$datastructure <- factor(varsize_data$datastructure, 
                                     levels=c("PLUSH","Dash","FASTER","PMEMKV","rocksdb","Viper"))


r90w10data <- sqldf("select * from throughput_data where workload='r90w10'")
r50w50data <- sqldf("select * from throughput_data where workload='r50w50'")
r10w90data <- sqldf("select * from throughput_data where workload='r10w90'")

r90w10data$throughput <- as.numeric(r90w10data$throughput)
r50w50data$throughput <- as.numeric(r50w50data$throughput)
r10w90data$throughput <- as.numeric(r10w90data$throughput)



r90w10 <- ggplot(r90w10data, aes(x=threadcount, y=throughput/1000000, color=datastructure)) + 
  geom_line() +
  geom_point(data= . %>%  filter(threadcount %in% c(1,2,4,8,16,24,36,48)), aes(shape=datastructure)) +
  scale_color_viridis_d(direction=1) + 
  xlab("\\# threads") +
  ylab("ops/s $\\times 10^6$") +
  scale_shape_manual(values=c(1,2,3,4,5,6,7,8,9)) +
  theme_bw() +
  annotate("rect", xmin = 24, xmax = Inf, ymin = -Inf, ymax = Inf,
           alpha = .2) +
  scale_x_continuous(breaks = c(1,4,8,16,24,36,48), limits = c(1,48), minor_breaks = NULL) + 
  theme(legend.position = "none") 



r50w50 <- ggplot(r50w50data, aes(x=threadcount, y=throughput/1000000, color=datastructure)) + 
  geom_line() +
  geom_point(data= . %>%  filter(threadcount %in% c(1,2,4,8,16,24,36,48)), aes(shape=datastructure)) +
  scale_color_viridis_d(direction=1) + 
  xlab("\\# threads") +
  theme_bw() +
  scale_shape_manual(values=c(1,2,3,4,5,6,7,8,9)) +
  #scale_y_continuous(breaks = c(0,2,4,6,8), limits = c(0,8.5)) + 
  annotate("rect", xmin = 24, xmax = Inf, ymin = -Inf, ymax = Inf,
           alpha = .2) +
  scale_x_continuous(breaks = c(1,4,8,16,24,36,48), limits = c(1,48), minor_breaks = NULL) + 
  theme(axis.title.y=element_blank(), legend.position = "none") 


r10w90 <- ggplot(r10w90data, aes(x=threadcount, y=throughput/1000000, color=datastructure)) + 
  geom_line() +
  geom_point(data= . %>%  filter(threadcount %in% c(1,2,4,8,16,24,36,48)), aes(shape=datastructure)) +
  scale_color_viridis_d(direction=1) + 
  xlab("\\# threads") +
  scale_shape_manual(values=c(1,2,3,4,5,6,7,8,9)) +
  theme_bw() +
  #ylim(0,6) +
  annotate("rect", xmin = 24, xmax = Inf, ymin = -Inf, ymax = Inf,
           alpha = .2) +
  scale_x_continuous(breaks = c(1,4,8,16,24,36,48), limits = c(1,48), minor_breaks = NULL) + 
  theme(axis.title.y=element_blank(), legend.position = "none") 

varsize <- ggplot(varsize_data, aes(x=key_size+val_size, y=throughput/1000000, color=datastructure)) + 
  geom_line(size=1 ) +
  geom_point(aes(shape=datastructure)) +
  scale_color_viridis_d(direction=1) + 
  scale_shape_manual(values=c(1,2,3,4,5,6,7,8,9)) +
  xlab("record size (k/v)") +
  scale_x_log10(breaks = c(16,32,144,288,576,1128), 
                     labels = c("8/8","16/16", "16/128", "32/256", "64/512", "\\ 128/1024"),
                     minor_breaks = NULL) +
  theme_bw() +
  theme(axis.text.x = element_text(size=5.5), axis.title.y=element_blank(), legend.position = "none")
  



plot(r90w10)
plot(r50w50)
plot(r10w90)
plot(varsize)

tikz(file = "varsize_r90w10.tex", width=tikzwidth, height=tikzheight)
show(r90w10)
dev.off()

tikz(file = "varsize_r50w50.tex", width=tikzwidth, height=tikzheight)
show(r50w50)
dev.off()

tikz(file = "varsize_r10w90.tex", width=tikzwidth, height=tikzheight)
show(r10w90)
dev.off()

tikz(file = "varsize_var.tex", width=tikzwidth, height=tikzheight)
show(varsize)
dev.off()

