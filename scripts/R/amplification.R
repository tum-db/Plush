library(ggplot2)
library(sqldf)
library(metR)
library(tikzDevice)
library(viridis)
library(ggforce)
library(ggpattern)

tikzheight <- 1.6
tikzwidth <- 3.5

#remotes::install_github("coolbutuseless/ggpattern")


amplification_data <- read.csv("../benchmarks/amplification.csv")

amplification_data["amplification"] <- amplification_data["bytes"] / (amplification_data["elemcount"] * 16)
amplification_data["bytesperop"] <- amplification_data["bytes"] / amplification_data["elemcount"]

amplification_data_logical <- sqldf("select * from amplification_data where type='logical'")
amplification_data_device <- sqldf("select * from amplification_data where type='device'")
amplification_data_logical_lookup <- sqldf("select * from amplification_data where type='logical' and operation='lookup'")
amplification_data_device_lookup <- sqldf("select * from amplification_data where type='device' and operation='lookup'")

amplification <- ggplot(amplification_data_logical, aes(x=rw, y=bytesperop/1024)) + 
  geom_bar(stat="identity", position="dodge", aes(fill=medium)) +
  geom_bar_pattern(stat="identity", position="dodge", data=amplification_data_device, aes(pattern_fill=medium), 
                   pattern = "stripe", color="black", fill="transparent", pattern_density = 0.05, pattern_size=0.1) +
  scale_fill_viridis_d(direction=1) + 
  xlab("") +
  ylab("KiB/op") +
  theme_bw() +
  facet_grid(operation ~ datastructure, labeller = label_wrap_gen(width=10)) +
  theme(strip.text.x = element_text(size = 6),
    legend.position = "none",
        plot.margin = unit(c(.1,.1,-.3,0), "cm"),
        strip.background = element_blank())



plot(amplification)

tikz(file = "amplification.tex", width=tikzwidth, height=tikzheight)
show(amplification)
dev.off()

