library(ggplot2)
library(sqldf)
library(metR)
library(tikzDevice)
library(viridis)
library(ggforce)

tikzheight <- 1.2
tikzwidth <- 3.25



recoverytime_data <- read.csv("../benchmarks/recoverytime.csv")


recoverytime <- ggplot(recoverytime_data, aes(x=(size*16)/(1024 * 1024 * 1024), y=ms/1000)) + 
  geom_area(aes(fill=type)) + 
  facet_zoom(xlim=c(0,40), ylim=c(0,0.5), zoom.size=0.5, horizontal=TRUE) +
  scale_fill_viridis_d(direction=-1) + 
  xlab("data set size [GiB]") +
  ylab("Recovery time [s]") +
  theme_bw() +
  theme(legend.margin=margin(t = 0.1, b=0.1, unit='cm'), legend.position = "top", legend.title = element_blank(),
        plot.margin = unit(c(.1,.1,.1,0), "cm"),
        legend.key.size = unit(2, 'mm'),
        legend.box.background = element_rect(colour = "black"))



plot(recoverytime)


tikz(file = "recoverytime.tex", width=tikzwidth, height=tikzheight)
show(recoverytime)
dev.off()
