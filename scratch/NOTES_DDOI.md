

[main fe3f78e] wip good progress on ddoi-gex

![image](https://github.com/user-attachments/assets/7bce2cea-73f6-4669-8364-75d6b38dcc5a)

![image](https://github.com/user-attachments/assets/d4a75fed-bf0f-492f-8fcf-7f0ea833d59d)


# notes

timeandsale event contains column aggressor_side 
which provides most likely the side of the trade.
per `hau volatility 2021`, if aggressor_side is UNDEFINED
then we can use change in order bodek
or derive theoretical iv curve from time of transaction
to guess/determine aggressor_side.

in `quote` event,
you have `tstamp bid_price	ask_price	bid_size	ask_size`
if ask_size drop, then likely aggressor is buy side.



diy intraday directionalized GEX of SPX on 2025-05-08 (bart head pattern due to Trump tweet)
Need to replot with major pos/neg gex levels and total gex by time

https://x.com/aigonewrong/status/1920572344256565633



