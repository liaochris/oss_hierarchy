Let $x_1, \ldots, x_T$ be training observations (**pre-period**) and $x_{T+1}, \ldots, x_{T+S}$ be evaluation observations (**post-period**), drawn from some true distribution $G$. I fit a parametric model $F$ to the training data; denote by $\mu_F$ and $\sigma_F^2$ the mean and variance **implied by** $F$. It's likely that $F \neq G$. Define the standardized residual and centered squared residual for any observation $x$ as

$$Z = \frac{x - \mu_F}{\sigma_F}, \qquad A = Z^2 - 1.$$

If $F$ is correctly specified, $Z \sim N(0,1)$ and $E[A] = 0$. In the pre-period, $\mu_F$ and $\sigma_F$ are estimated leaving out the observation being scored (leave-one-out), so $Z_t^{pre}$ is an out-of-sample prediction on the same footing as the post-period.

### Problem: $A$ mixes modelling error and prediction error

Even if the post-period data follow the same distribution as the training data, $A_s^{post}$ need not be zero — it is also inflated whenever $F$ is a poor fit to the true distribution $G$. Conversely, a large $A_s^{post}$ could reflect the fact that the post-period data is drawn from a different distribution, or model misspecification that was already present in the pre-period. 

### Solution: difference out the pre-period baseline

Define the **differenced residual**:

$$\tilde{A}_s = A_s^{post} - \bar{A}^{pre}, \qquad \bar{A}^{pre} = \frac{1}{T}\sum_{t=1}^T A_t^{pre}.$$

By subtracting the average pre-period $A$, we remove the component that is common to both periods — namely, the baseline model misfit. What remains reflects only changes between pre- and post-period.

### Justification

Let $G$ be the true data-generating distribution. For any $x \sim G$ standardized by the model-implied $(\mu_F, \sigma_F)$:

$$E_G[A] = E_G\!\left[\left(\frac{x - \mu_F}{\sigma_F}\right)^2\right] - 1 = \frac{(\mu_G - \mu_F)^2 + \sigma_G^2}{\sigma_F^2} - 1 \;=:\; \Delta(F, G).$$

The **baseline bias** $\Delta(F, G)$ depends only on the discrepancy between $F$ and $G$, not on which period we are in. Therefore, if pre- and post-period data are both drawn from the same $G$ (no distributional shift):

$$E[\tilde{A}_s] = \Delta(F, G) - \Delta(F, G) = 0,$$

regardless of how misspecified $F$ is. If instead the post-period distribution shifts to $G'$:

$$E[\tilde{A}_s] = \Delta(F, G') - \Delta(F, G) \neq 0.$$

The differenced residual is therefore **insensitive to model misspecification** and nonzero only when the post-period distribution genuinely differs from the pre-period.
