# model.tex improvements (up to Prediction section)

File: `source/paper/model.tex`

## Changes

1. **Rename `\subsection{Baseline model}`** → something like `\subsection{Organizational workflow}`. The section sets up problem flow and stage transitions, not a "baseline" in the econometric sense.

2. **Move assumptions before first use.**  
   Currently all assumption environments live in the late `\subsection{Structural assumptions}` but are referenced earlier. Instead:
   - Define Assumptions 1–3 (Exchangeability, Problem independence, Memoryless) inline before the transition matrix.
   - Define Assumption 4 (Problem-invariant member probabilities) inline before the opening multinomial.
   - Define Assumption 5 (Independent review decisions) inline before the review binomial.
   - Remove the standalone `\subsection{Structural assumptions}` section.

3. **Write $N_{i,t}^o$, $N_{i,t}^r$, $N_{i,t}^m$ as expectations.**  
   Since $N_{i,t} \sim F_{it}$ is random, replace the current `$N_{i,t}^o = N_{i,t}\cdot\pi_{i,t}^o$` identities with proper expectations: $\mathbb{E}[N_{i,t}^o] = \mathbb{E}[N_{i,t}]\cdot\pi_{i,t}^o$, etc. Update the lead-in prose accordingly.

4. **Move motivation sentence to open §1.2.**  
   "My goal is to predict how organizational activity changes when a member leaves..." currently ends §1.1. Move it to the start of the Bernoulli-actions subsection.

5. **Promote `\subsubsection` → `\subsection`** for "Opening, review, and merge as person-specific Bernoulli actions."

6. **Define individual-level counts $A_{j,t}^o$, $A_{j,t}^r$, $A_{j,t}^m$ at the top of the new §1.2**, before their distributions are derived.

7. **Tighten §1.1 prose** for space efficiency without sacrificing clarity.
