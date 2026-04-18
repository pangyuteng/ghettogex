

+ in "/debug" finish implmenting the volume chart
  + use uplot for volume plot, see the first "Latency Heatmap (~35k)" as example: https://github.com/leeoniya/uPlot/blob/master/demos/latency-heatmap.html
  + for sure you have to edit function "process_volume_data".
  + if you have design pattern doubts, follow the "expectedmove" design pattern that is present in `/debug`.
  + you need to also grab the spot ticker price and overlay on top of the volume plot.
  + spot price line will be black
  + volume will be a gradient from black to bright orange. min is 0, max is 2500.

