


# maybe start looking into proper but fast methods:
# easy to read good intro from non-finance author https://www2.math.ethz.ch/EMIS/journals/DRNA/pdf/05_cuomo.pdf
# https://quant.stackexchange.com/questions/77141/what-is-the-point-of-sabr-model-as-an-interpolation-tool-if-we-can-already-obser
# https://quant.stackexchange.com/questions/68141/fitting-a-volatility-smile-with-pysabr-python-implementation-of-sabr-model


# naive bad
https://www.stephendiehl.com/posts/volatility_surface/

# SVI SABR


https://www.reddit.com/r/algotrading/comments/lqpoqi/best_way_to_fit_implied_volatility_to_a_surface/
https://emanuelderman.com/wp-content/uploads/1996/06/gs-local_volatility_surface.pdf
https://docs.scipy.org/doc/scipy/reference/generated/scipy.interpolate.RBFInterpolator.html

https://numba.pydata.org/numba-doc/dev/index.html

https://medium.com/@alexander.tsoskounoglou/pricing-options-with-fourier-series-p3-the-heston-model-d157369a217a
corresponding notebook https://github.com/ithakis/Pricing-Options-with-Black-Scholes/blob/main/Heston%20Model%20Calibration.ipynb


https://github.com/JunbeomL22/SurfaceFitting/blob/master/Code/SurfaceFitting.py
https://github.com/JunbeomL22/SurfaceFitting/blob/master/Code/ssvi.py

https://github.com/clf110510/stochastic-volatility
https://github.com/ynouri/pysabr

https://colab.research.google.com/drive/1M1YJncdswd-A9SgIOAjw6g6Se7NHU9mG?usp=sharing

https://www.quantlib.org/

https://www.quantconnect.com/learning/articles/introduction-to-options/local-volatility-and-stochastic-volatility

https://quant.stackexchange.com/questions/73488/pricing-a-digital-barrier-option-using-quantlib-in-python
https://quant.stackexchange.com/questions/44300/mixed-local-stochastic-volatility-model-in-quantlib
https://quant.stackexchange.com/questions/51749/heston-volatility-surface-in-python-quantlib

https://www.linkedin.com/pulse/creating-volatility-surface-quantlib-colman-marcus-quinn-ehoye/
https://quantlib-python-docs.readthedocs.io/en/latest/stochastic_processes.html#ql.HestonSLVProcess

good example: https://github.com/ynouri/pysabr/pull/7/files
https://github.com/ynouri/pysabr/blob/master/notebooks/Lognormal%20SABR%20vs%20Normal%20SABR.ipynb

https://www.pyquantnews.com/the-pyquant-newsletter/calibrating-volatility-smiles-with-sabr

finally back to pysabr
https://quant.stackexchange.com/questions/68141/fitting-a-volatility-smile-with-pysabr-python-implementation-of-sabr-model
https://greektrader.wordpress.com/2020/08/29/sabr-volatility-model/


pysabr 0dte volatility surface

https://quant.stackexchange.com/questions/77365/fitting-volatility-using-sabr
"There is no way SABR will be able to match this shape. It's only a few parameters after all. – 
user70573 CommentedJan 6, 2024 at 0:43"

https://quant.stackexchange.com/questions/67889/cant-fit-bloomberg-volatility-smile-with-pysabr-what-am-i-doing-wrong

https://dybeta2021.github.io/2021/07/13/svi/

https://github.com/wangys96/SVI-Volatility-Surface-Calibration/tree/master


https://quant.stackexchange.com/questions/31151/which-models-do-bloomberg-reuters-use-to-derive-implied-volatility-for-interest/74179#74179

**COOLIOS!! below **
https://quant.stackexchange.com/questions/76366/option-pricing-for-illiquid-case/76367#76367
 https://quant.stackexchange.com/q/49034/54838
 https://quant.stackexchange.com/questions/74325/svi-calibration/74336#74336
https://github.com/wangys96/SVI-Volatility-Surface-Calibration/blob/master/svi.py
https://github.com/wangys96/SVI-Volatility-Surface-Calibration/blob/master/SVI_Calibration.ipynb
https://quant.stackexchange.com/questions/19344/how-to-calibrate-a-volatility-surface-using-svi
https://quant.stackexchange.com/questions/16909/local-volatility-svi-parametrization
https://github.com/mChataign/Beyond-Surrogate-Modeling-Learning-the-Local-Volatility-Via-Shape-Constraints/blob/Marc/GP/code/SSVI.py

https://quant.stackexchange.com/questions/59396/question-about-svi-and-ssvi-tradeoff-between-fitness-and-no-arbitrage
https://github.com/JackJacquier/SSVI

SSVI SVI

https://quant.stackexchange.com/questions/73861/is-it-possible-to-have-only-one-volatility-surface-for-american-options-that-fi
https://quant.stackexchange.com/questions/55545/implied-vol-smile-from-calls-puts-or-both?noredirect=1&lq=1

keywords: SSVI volatility surface python

https://github.com/JunbeomL22/SurfaceFitting/tree/master

https://github.com/search?q=SSVI+volatility&type=repositories

https://github.com/domokane/FinancePy