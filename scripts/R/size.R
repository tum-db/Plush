library(ggplot2)
library(sqldf)
library(metR)
library(tikzDevice)
library(data.table)
library(dplyr)
library(patchwork)
library(viridis)
library(ggforce)

tikzheight <- 1.3
tikzwidth <- 3.25


# Note: For this experiment it is necessary to increase the pool size of the different data structures
# to the maximum space available on your PMEM partition.
# This is toned down for the normal benchmark as we do not want to hog all the space during normal operation.

size_data <- read.csv("../benchmarks/size.csv")

size_data$datastructure <- factor(size_data$datastructure, 
                                        levels=c("PLUSH","Plush w/o DRAM-filter","DASH","utree","FPTree", "FASTFAIR","DPTree", "Viper"))



dram_size <- ggplot(size_data, aes(color=datastructure, x=(elemcount*16)/(1024 * 1024 * 1024), y=dram_size/(1024*1024))) + 
  geom_line() + 
  geom_point(data= . %>%  filter(elemcount %% 2000000000 == 0), aes(shape=datastructure)) +
  scale_color_viridis_d(direction=1) + 
  scale_shape_manual(values=c(1,2,3,4,5,6,7,8,9)) +
  xlab("data set size [GiB]") + 
  ylab("DRAM [GiB]") +
  facet_zoom(xy = (elemcount*16)/(1024 * 1024 * 1024) <= 100 & dram_size/(1024*1024) <= 5, zoom.size=0.5, horizontal=TRUE) +
  theme_bw()+
  theme(legend.position = "none", plot.margin = unit(c(.1,.1,.1,0), "cm"))

pmem_size <- ggplot(size_data, aes(color=datastructure, x=(elemcount*16)/(1024 * 1024 * 1024), y=(dram_size+pmem_size)/(1024*1024))) + 
  geom_line() + 
  geom_point(data= . %>%  filter(elemcount %% 2000000000 == 0), aes(shape=datastructure),position=position_dodge(width=1)) +
  scale_color_viridis_d(direction=1) + 
  scale_shape_manual(values=c(1,2,3,4,5,6,7,8,9)) +
  xlab("data set size [GiB]") + 
  ylab("DRAM\\&PMem [GiB]") +
  geom_abline(slope=1, intercept = 0) +
  theme_bw()+
  theme(axis.title.y=element_text(size=8), legend.position = "none", plot.margin = unit(c(.1,.1,.1,0), "cm"))




dev.off()

tikz(file = "dram_size.tex", width=tikzwidth, height=tikzheight)
dram_size

dev.off()

tikz(file = "total_size.tex", width=tikzwidth, height=tikzheight)
pmem_size

dev.off()

show(load_factor)
show(dram_size)
show(pmem_size)
