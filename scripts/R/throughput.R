library(ggplot2)
library(sqldf)
library(metR)
library(tikzDevice)
library(viridis)
library(ggforce)
library(dplyr)

tikzheight <- 1.3
tikzwidth <- 1.45




throughput_data <- read.csv("../benchmarks/throughput.csv")
insertratio_data <- read.csv("../benchmarks/insertratio.csv")
skew_data <- read.csv("../benchmarks/skew.csv")

throughput_data$datastructure <- factor(throughput_data$datastructure, 
                                        levels=c("PLUSH","Plush w/o DRAM-filter","DASH","utree","FPTree", "FASTFAIR","DPTree", "Viper"))

insertratio_data$datastructure <- factor(insertratio_data$datastructure, 
                                        levels=c("PLUSH","Plush w/o DRAM-filter","DASH","uTree","FPTree", "FASTFAIR","DPTree", "Viper"))

skew_data$datastructure <- factor(skew_data$datastructure, 
                                         levels=c("PLUSH","Plush w/o DRAM-filter","DASH","uTree","FPTree", "FASTFAIR","DPTree", "Viper"))
skew_data$skew <- factor(skew_data$skew, levels=c("0.1","0.2","0.5","0.7","0.9", "0.99","0.999"))


insert_data <- sqldf("select * from throughput_data where operation='insert'")
lookup_data <- sqldf("select * from throughput_data where operation='lookup'")
delete_data <- sqldf("select * from throughput_data where operation='delete'")
scan_data <- sqldf("select * from throughput_data where operation='scan'")


insert_data$throughput <- as.numeric(insert_data$throughput)
insert <- ggplot(insert_data, aes(x=threads, y=throughput/1000000, color=datastructure)) + 
  geom_line() +
  geom_point(data= . %>%  filter(threads %in% c(1,2,4,8,16,24,36,48)), aes(shape=datastructure)) +
  scale_color_viridis_d(direction=1) + 
  scale_shape_manual(values=c(1,2,3,4,5,6,7,8,9)) +
  xlab("\\# threads") +
  theme_bw() +
  annotate("rect", xmin = 24, xmax = Inf, ymin = -Inf, ymax = Inf,
           alpha = .2) +
  scale_x_continuous(breaks = c(1,4,8,16,24,36,48), limits = c(1,48), minor_breaks = NULL) + 
  theme(axis.title.y=element_blank(), legend.position = "none")


lookup <- ggplot(lookup_data, aes(x=threads, y=throughput/1000000, color=datastructure)) + 
  geom_line() +
  geom_point(data= . %>%  filter(threads %in% c(1,2,4,8,16,24,36,48)), aes(shape=datastructure)) +
  scale_color_viridis_d(direction=1) + 
  scale_shape_manual(values=c(1,2,3,4,5,6,7,8,9)) +
  xlab("\\# threads") +
  ylab("ops/s $\\times 10^6$") +
  theme_bw() +
  annotate("rect", xmin = 24, xmax = Inf, ymin = -Inf, ymax = Inf,
           alpha = .2) +
  scale_x_continuous(breaks = c(1,4,8,16,24,36,48), limits = c(1,48), minor_breaks = NULL) + 
  theme(legend.position = "none") 

skew <- ggplot(skew_data, aes(x=skew, y=throughput/1000000, group=datastructure, color=datastructure)) + 
  geom_line() +
  geom_point(aes(shape=datastructure)) +
  scale_color_viridis_d(direction=1) + 
  scale_shape_manual(values=c(1,2,3,4,5,6,7,8,9)) +
  xlab("skew") +
  ylim(0,90) +
  theme_bw() +
  theme(axis.text.x = element_text(size=6), axis.title.y=element_blank(), legend.position = "none") 


delete <- ggplot(delete_data, aes(x=threads, y=throughput/1000000, color=datastructure)) + 
  geom_line() +
  geom_point(data= . %>%  filter(threads %in% c(1,2,4,8,16,24,36,48)), aes(shape=datastructure)) +
  scale_color_viridis_d(direction=1) + 
  scale_shape_manual(values=c(1,2,3,4,5,6,8)) +
  xlab("\\# threads") +
  theme_bw() +
  annotate("rect", xmin = 24, xmax = Inf, ymin = -Inf, ymax = Inf,
           alpha = .2) +
  scale_x_continuous(breaks = c(1,4,8,16,24,36,48), limits = c(1,48), minor_breaks = NULL) + 
  theme(axis.title.y=element_blank(), legend.position = "none") 


scan <- ggplot(scan_data, aes(x=threads, y=throughput/1000000, color=datastructure)) + 
  geom_line() +
  geom_point(data= . %>%  filter(threads %in% c(1,2,4,8,16,24,36,48)), aes(shape=datastructure)) +
  scale_color_viridis_d(direction=1) + 
  scale_shape_manual(values=c(1,2,3,4,5,6,7,8,9)) +
  xlab("\\# threads") +
  ylab("ops/s $\\times 10^6$") +
  theme_bw() +
  annotate("rect", xmin = 24, xmax = Inf, ymin = -Inf, ymax = Inf,
           alpha = .2) +
  scale_x_continuous(breaks = c(1,4,8,16,24,36,48), limits = c(1,48), minor_breaks = NULL) + 
  theme(legend.position = "none") 



insertratio <- ggplot(insertratio_data, aes(x=insertratio, y=throughput/1000000, color=datastructure)) + 
  geom_line() +
  geom_point(aes(shape=datastructure)) +
  scale_color_viridis_d(direction=1) + 
  scale_shape_manual(values=c(1,2,3,4,5,6,7,8,9)) +
  xlab("insert ratio") +
  ylim(0,52) +
  theme_bw() +
  theme(axis.title.y=element_blank(), legend.position = "none") 


plot(insert)
plot(lookup)
plot(delete)
plot(scan)

plot(insertratio)
plot(skew)


tikz(file = "insert.tex", width=tikzwidth, height=tikzheight)
show(insert)
dev.off()

tikz(file = "lookup.tex", width=tikzwidth, height=tikzheight)
show(lookup)
dev.off()

tikz(file = "skew.tex", width=tikzwidth, height=tikzheight)
show(skew)
dev.off()

tikz(file = "delete.tex", width=tikzwidth, height=tikzheight)
show(delete)
dev.off()

tikz(file = "scan.tex", width=tikzwidth, height=tikzheight-0.3)
show(scan)
dev.off()

tikz(file = "insertratio.tex", width=tikzwidth, height=tikzheight)
show(insertratio)
dev.off()

