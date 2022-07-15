library(ggplot2)
library(sqldf)
library(metR)
library(tikzDevice)
library(viridis)
library(ggforce)

tikzheight <- 1.1
tikzwidth <- 3.25



latencies <- read.csv("../benchmarks/latencies.csv")


latencies$percentile <- factor(latencies$percentile, levels=c("min","50\\%","90\\%","99\\%", "99.9\\%","99.99\\%","99.999\\%"))
latencies$datastructure <- factor(latencies$datastructure, 
                                  levels=c("PLUSH","Plush w/o DRAM-filter","DASH","utree","FPTree", "FASTFAIR","DPTree", "Viper"))

latencies_plot <- ggplot(latencies, aes(x=percentile, y=latency_ns/1000, group=datastructure, color=datastructure)) + 
  geom_line() + 
  geom_point(aes(shape=datastructure)) +
  scale_color_viridis_d(direction=1) + 
  scale_shape_manual(values=c(1,2,3,4,5,6,7,8,9)) +
  facet_grid(cols=vars(operation), scales = "free_y") +
  coord_cartesian(ylim=c(0, 100)) +
  xlab("") +
  ylab("latency [$\\mu$s]") +
  theme_bw() +
  theme(legend.margin=margin(t = 0.1, b=0.1, unit='cm'), legend.position = "none", legend.title = element_blank(),
        plot.margin = unit(c(.1,.1,-.5,0), "cm"),
        legend.key.size = unit(2, 'mm'),
        legend.box.background = element_rect(colour = "black"),
        axis.text.x = element_text(angle = 35, vjust = 1, hjust=.7),
        strip.background = element_blank())


show(latencies_plot)


tikz(file = "latencies.tex"), width=tikzwidth, height=tikzheight)
show(latencies_plot)
dev.off()
