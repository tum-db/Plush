library(ggplot2)
library(sqldf)
library(metR)
library(tikzDevice)
library(viridis)
library(ggforce)

tikzheight <- 1.3
tikzwidth <- 1.6

#Note: This file was generated manually by changing the parameters
# MAX_DRAM_FILTER_LEVEL, MAX_BUCKET_PREALLOC_LEVEL and logging
# in the Plush source code!


improvements_data <- read.csv("../benchmarks/improvements.csv")


improvements_data$configuration <- factor(improvements_data$configuration, levels = c("baseline", "+prealloc", "+dramfilter","-log"))

improvements <- ggplot(improvements_data, aes(x=configuration, y=throughput/1000000)) + 
  geom_bar(position="dodge", stat="identity") +
  scale_fill_viridis_d(direction=-1) + 
  xlab("") +
  ylab("ops/s $\\times 10^6$") +
  theme_bw() +
  facet_grid(cols = vars(operation)) +
  theme(legend.margin=margin(t = 0.1, b=0.1, unit='cm'), legend.position = "top", legend.title = element_blank(),
        plot.margin = unit(c(.1,.1,-.5,0), "cm"),
        legend.key.size = unit(2, 'mm'),
        legend.box.background = element_rect(colour = "black"),
        axis.text.x = element_text(size=6, angle = 30, vjust = 1, hjust=1),
        strip.background = element_blank())




plot(improvements)

tikz(file = "improvements.tex", width=tikzwidth, height=tikzheight)
show(improvements)
dev.off()
