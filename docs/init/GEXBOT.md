# gexbot classic

Gexbot classic displays gex by OI and gex by volume in a ladder-style histogram. For each strike, call gex is netted out against put gex. Bars extending to the right represent call gex. Put gex is indicated by bars extending to the left.
The plot embeds ‘lookback’ dots in the histogram to visualize gex values at various intervals into the past. It also includes a slider to track through the history of the day.

Gex by volume is used to calculate:

zero gamma (the center of the complex)
major positive gamma (the positive GEX strike of the greatest magnitude)
major negative gamma (the negative GEX strike of the greatest magnitude)
max change strikes (the strikes with the most change in GEX over the last 1, 5, 15, and 30 min)
A side panel also includes zero gamma, major positive gamma, and major negative gamma as calculated by OI.
We run three versions:
full reflects all expires within 90 days.
latest reflects the nearest expiry.
next reflects the following expiry.

# options profile (OP)

Description:
Our options profile displays the results of our orderflow-classification engine. Options profile is available for the latest expiry and the following one ("next"). Our classification distinguishes between customer long options and customer short options. We do not distinguish between orders to open and orders to close.
Puts default to purple.
Calls default to orange.
Long options extend to the right.
Short options extend to the left.
The applicable features of gexbot classic (lookback dots, history slider) are included here too, with classified volume data as the backbone.

One feature that differs from classic is the addition of 0DTE volatility skew. The red and green dots on the chart represent the implied volatilities of puts and calls respectively. The skew axis is a secondary x axis which can be found at the top of the chart.
Reading the OP:
The very first thing to keep in mind is that significant strikes represent areas of concentrated liquidity. As such they represent "stops" on our roadmap. But what about direction?

If customers are long, they are incentivized to exit those positions. If customers are short, they are incentivized to maintain their positions, as theta decay and volatility compression will bleed value out of their short contracts.

For the poker fanatics out there: Holding on to long contracts is -EV (expected value), whereas holding on to short contracts is +EV over the long run. This has a very specific effect:

Long options act like walls. When the price gravitates towards them, holders are likely to liquidate, providing liquidity and stifling movement.
Short options do the opposite. As the price gravitates towards them, holders will likely need to hedge, taking liquidity out of the market and increasing the likelihood of continuation.
One additional variable is worth mentioning here: volatility.

As volatility rises, contract prices rise too.
As volatility compresses, contract prices fall. The above holds in a declining volatility environment (which is the most common).
A rising volatility environment, however, decreases the incentive to exit long options positions, making walls more vulnerable.
A rising volatility environment does not alter (and may even increase) the incentive to hold on to short options positions. Sellers are being paid more to hold on, as volatility premiums rise.
Often, sellers will even add to their short inventory as we approach a short strike. Thus, in a rising volatility environment, short options are more likely to act as walls.

# gex profile

The gex profile takes the results of our orderflow-classification engine and isolates the greatest inflection points in the market. Here's how we do it:

1. The OP measures the net imbalance of transactions so far that day. What this means is that if one customer buys an option and another customer sells it, it won't show up on our chart. If a dealer buys an option, and then sells it, it also won't show up on our chart. If there's excess demand or excess supply for a contract, it will show up.

2. The typical assumption regarding options positioning is that dealers will delta hedge dynamically and that customers won't, which leads to a myopic focus on dealer positioning. But customers in aggregate will reposition themselves in line with their optimal incentives. So only thinking in terms of dealer positioning only gives you half of the truth.

3. Wherever there is a net imbalance, there is a seller on the other side (regardless of dealer/customer) who will be forced to adjust. If there is a net imbalance of puts equal to calls, then those sellers would get squeezed in opposite directions at the same time.

4. Gex profile therefore nets out imbalanced calls against imbalanced puts (regardless of dealer/customer) to locate areas where there is imbalanced exposure. We display call gex imbalance to the right and put gex imbalance to the left. This helps us quickly distinguish between high gamma nodes (targets), low gamma nodes (transition areas), and call/put gamma regimes.

We run two versions, both with the applicable features of gexbot classic (lookback dots, history slider, major levels):
full reflects classified orderflow on all expires within 90 days.
latest reflects classified orderflow in the nearest expiry.
next reflects classified orderflow in the following expiry.

# dex ladder

Options profile can be a lot of information to take in at once. The dex ladder takes the net imbalance of transactions so far that day and displays the delta exposure at each strike.
Customer long calls and short puts have positive delta, while customer short calls and long puts have negative delta. Instead of having two bars per strike, dex allows us to have one.
It shows us where participants are short, where they are long, and by how much. We can anticipate which levels will be heavily defended, and which levels will be decisive once overcome. Transition points between heavy short interest and traders attempting longs, for instance, signal targets and possible reversion zones. 
The dex ladder is the options version of the current order book. Heavy short interest coming in above spot is a warning sign (much like a passive limit seller coming in above spot), just as heavy long interest (passive limit buyer) coming in below can be supportive.
On underlyings, like SPY, for which options are most often traded directionally, a glance at the dex ladder can also be a quick way of developing a bias: How aggresively are traders positioned today?
One additional advantage is worth noting: In higher volatility environments, the gamma curve flattens out, strengthening the relationship between dex and price movement. More simply, the delta of a transaction becomes increasingly relevant as liquidity dries up. Accordingly, we rely more on dex as things heat up.

# convexity ladder

The convexity ladder takes the net imbalance of transactions so far that day and displays the net gamma exposure of those positions. Long calls and long puts represent long customer gex, while short calls and short puts represent short customer gex.

We use convexity rather than the simpler "gex ladder" because we want to convey the chart's significance in a more visceral way:

Positive convexity: Customers own convexity, favoring moves that exceed expectations. Ideally, they want price traveling as far away from their positive strikes as possible.
Negative convexity: Customers are short convexity, favoring moves that underperform expectations. Ideally, they favor price approaching and resting at their negative strikes.
Reading the convexity ladder is less about discerning direction, and more about measuring risk. For example, our favorite SPY setup (infrequent, but explosive) occurs when a single strike of negative convexity dwarfs all others as spot hovers just above. Being short convexity at the wrong time in a crowded place can cause cascading panic, if given a gentle shove. Perhaps no shove occurs, but one can be ready in case it does.

# vanna/charm ladder

vanna and charm ex are in beta

Why? Options profile classifies intraday transactions rather than keeping tabs on total open inventory (eventually gexbot will do both). We care about what customers do and how they respond to price. Vanna and charm effects are dealer heavy, so we are still learning best practices when applied to daily classifications.

With that caveat in mind, we can get to the good stuff. As our metrics page pointed out, both our vanna and charm ladders indicate the delta impact, in dollar terms, of the collapse in volatility and the passage of time respectively. While - vanna ex estimates the impact of a total collapse (i.e. all options expiring), charm ex estimates the impact per hour. This is why our charm ex. increases exponentially into expiration: the decay rate of OTM options approaches infinity as time approaches zero. While the - vanna ex is more stable, charm ex highlights the relevance of shorter dated options as an estimate of hedging flows into expiry.

In both cases, a bar to the right indicates that customers will get longer (gain deltas) into expiration whereas a bar to the left indicates they will get shorter (lose deltas) into expiration. Dealer flows are then straightforward–where customers get longer, dealers get shorter. Bars to the right indicate that dealers will get shorter going into expiration, meaning they will be forced to buy to stay neutral. Bars to the left indicate that dealers will get longer into expiration, meaning they will be forced to sell to stay neutral. Bluntly: bars to the right will be a source of supportive flows into expiry, whereas bars to the left will be a source of passive selling. The relevance of these flows, as mentioned above, grows exponentially into expiry–so watch for the last half hour of the trading day.

Positive bars: bullish passive flows into expiry
Negative bars: bearish passive flows into expiry
Reading the Ladder
Let's get more concrete. In general you'll notice that the vanna/charm of a customer bought options position (regardless of put/call) flips from from negative below spot to positive above spot into expiry. Essentially, it forces spot price away from itself. The vanna/charm of a customer sold options position (regardless of put/call) flips from positive below spot, to negative above! So large customer sold options positions actually attract price into expiry. The vanna/charm ladders give us a way to visualize this dynamic from the convexity ladder in action.

As the day winds to a close, however, vanna levels become more powerful than gamma. Positive vanna levels act as support from below, and negative vanna levels act as resistance from above. That means if we enter into a stack of negative vanna, we are likely to continue progressing downwards. If we enter into a stack of positive vanna, we are likely to continue progressing upwards. Keep in mind that the polarity of a vanna level will flip as spot crosses it, and zeros out when spot is on it. From a theoretical perspective, when spot is at a vanna level (local zerovanna), no hedging is needed for a given change in volatility. But as spot peeks slightly above or below, a substantial change is needed, which is what gives these levels their acuity. Significant levels represent local zeros for the vanna complex, making them strong pivots.

# orderflow

Time-based charts leverage the results of our orderflow-classification engine to give real-time insight into the nearest and following expiries.

Orderflow subplots detect changes in overall positioning: Any time an order comes through, we measure and report the delta and gamma of that order, and whether it is long or short. Orderflow helps to highlight transition points, both in terms of market direction and volatility.

“Net” subplots sum across all strikes, meaning that they give us a snapshot of total positioning at any given point in time. They help us get a sense for whether the options complex is in line with price action.

In following our options profile, all plots and equations are in terms of paper (customer) positioning.

#  dex orderflow

= (bullish volume × dex) - (bearish volume × dex)

Where:

Bullish volume = long call | short put
Bearish volume = short call | long put
Description:
Dex orderflow tells us when big orders are coming into the market in terms of their directional share-equivalent. It is as simple to read as: “Someone just bought/sold this many shares worth of options here”.
Significant dex orderflow highlights levels of interest. Large transactions tell us that aggressive positions are being established, or that liquidations are taking place.
Often, a local bottom will be established as an aggressive buyer enters the picture. Just as easily, a local bottom can be marked by bearish orderflow, as a long is forced to liquidate.


# gex orderflow

= (call gex imbalance) - (put gex imbalance)

Description:
Gex orderflow is designed to monitor changes in the gex profile. A bar up indicates that call gex imbalance has grown (more green is present on the gex profile), whereas a bar down indicates that put gex imbalance has grown (more red is present on the gex profile).
Positive gex orderflow brings us closer to a call dominated gamma environment. This isn't a simple buy signal. It adds convexity to our upside (indicating that if we were to rise, we would go farther), while elevating the probability that spot price will fall. It changes the shape of our expected distribution, making upside less likely, but more explosive. The opposite is true for negative gex orderflow.
Practically speaking, gex orderflow will mark high 𝛄 transactions, which are riskier and feature greater payoffs. As such it marks high conviction pivots.
On an index like SPX, which is dominated by short gamma, positive gex orderflow (often call selling) typically marks local tops, whereas negative gex orderflow marks local bottoms.

# convexity orderflow

= long orderflow × gex - short orderflow × gex

Description:
The intended use of convexity orderflow is to cross reference it against dex|gex orderflow. A transaction with positive dex|gex but negative convexity is a short put. If the same transaction has positive convexity, it must be a long call. In this way, one can monitor the optionsprofile without looking at the ladder chart.
More generally, convexity orderflow indicates when participants are wagering on more/less near-term volatility:
Positive convexity means that participants are expecting more volatility (buying options).
Negative convexity means that participants are expecting less volatility (selling options).
Sell-offs are often marked by consistent positive convexity orderflow (as there is demand for volatility). Whereas grinding days and squeezes often feature consistently negative convexity orderflow.

# net gex

net gex = total call gex imbalance - total put gex imbalance

Understanding net gex:
Net gex condenses the gex profile into a single measure.
When net gex is high and above 0, there are more holders of call 𝛄 than put 𝛄.
When net gex is low and below 0, there are more holders of put 𝛄 than call 𝛄.
𝛄 is a measure of convexity. If it is high, that means there is more upside convexity than downside convexity, and vice versa.
Market Implications:
If upside convexity is high and increasing (net gex is very positive), and bars are relatively equal, a squeeze is likely. If upside convexity is high and rising, and a single bar above price predominates, look for reversion there.
If downside convexity is low and decreasing (net gex is very negative), and bars are relatively equal, a selloff is likely. If downside convexity is low and decreasing, and a single bar below price predominates, look for reversion there.
Reading net gex in tandem with net convexity can be quite powerful. It can tell you at a glance whether there is more gamma exposure bought/sold as calls or puts.


# net convexity

= total customer long gex - total customer short gex

Description:
Net convexity is a measure of option buying vs. option selling.
Positive (high) net convexity means that participants are expecting more near-term volatility than is currently priced into the market.
Negative (low) net convexity means that participants are expecting less near-term volatility than is currently priced into the market.
Due to the inverse correlation between underlying price and implied volatility, low convexity is mildly constructive on underlying price. Very high net convexity, which indicates high demand for options, usually only occurs during times of panic.

# aggregate dex

= cumulative dex orderflow *
= cumulative call dex orderflow *
= cumulative put dex orderflow *

Description:
Aggregate dex tells us how much buying/selling the option market has contributed over the course of the day.
Instead of “Someone just bought/sold this many shares worth of options here”, it's: “This many shares worth of options have been bought/sold so far today”.
Negative call aggdex implies more call deltas sold than bought, and vice versa for puts. So, when call aggdex is negative and put aggdex is positive, participants have been broadly shorting volatility over the course of the day.
Different underlyings trade differently. In a lower volatility environment, SPX is primarily a short volatility and hedging instrument, so we often see positive put aggdex, negative call

# net vanna/charm

net vanna and charm are in beta (see vanna/charm ladders)

net charm = charm ex across all strikes ($MM/hr)
net vanna = - vanna ex across all strikes ($MM until expiry)

Net vanna and charm approximate the magnitude of the passive hedging pressure generated by transactions so far that day as we progress into expiration:

When positive, they indicate that customers will gain deltas (get longer) as we approach expiry.
When negative, they indicate that customers will lose deltas (get shorter) as we approach expiry.
Example:
Assume that the only positions on our ladder are customer sold OTM calls (they are short). As we progress into expiry, the value of those calls evaporates, making customers less short. In this case, both net charm and net vanna would be positive, indicating that customers will get longer as expiry approaches. Dealers will be getting shorter, causing them to buy to stay neutral. We would accordingly expect bullish passive flows from dealers into expiry. These flows will cease if/when price approaches those short calls and they become at-the-money contracts.

Timing and Magnitude:
While the above is simple and intuitive, the reality is that both timing and magnitude matter. On SPX, net vanna becomes relevant when beyond $800MM in magnitude and during the last hour of the day. When above $1000MM in magnitude, the effects become relevant sooner–in the last couple of hours. Getting a sense for these thresholds is a matter of some discretion, but we've noticed a marked difference between extreme and moderate values. Extreme days typically feature extreme intraday reversals as traders frontrun the effects of positioning unwinding. In these cases, you'll see net vanna, and net charm in particular, accompany and accelerate in the direction of the move.

Potential Energy:
This brings me to my final point, which is that both metrics are a good indication of potential energy in the market. When they hover at neutral, we are floating freely. We often see price stall or wander aimlessly during these times. Extreme values, however, beget extreme moves, even earlier in the day. It is only as the day progresses, though, that we feel comfortable taking a directional stance on the basis of these flows.