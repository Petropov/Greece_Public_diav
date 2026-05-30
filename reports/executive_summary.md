# Procurement Transparency in Lamia: Executive Summary
*Analysis of Diavgeia public procurement records, 2020–2026*
*Data: Diavgeia OpenData API · Coverage: search exports + targeted detail hydration*
*Verified: May 2026 — complete analysis with ΑΝΑΛΗΨΗ cross-check and supplier identification*

---

This analysis covers six years of public procurement records (2020–2026) for three public entities in Lamia — the General Hospital (ΓΝ Λαμίας), the municipality (Δήμος Λαμίας), and the water utility (ΔΕΥΑ Λαμίας) — cross-validated against peer institutions of comparable size in Trikala, Karditsa, Kozani, and Serres.

**The core finding is not a single anomaly but a structural pattern:** all three Lamia entities independently show elevated direct-award frequency. The most analytically clean signal is the hospital's **competitive award (ΚΑΤΑΚΥΡΩΣΗ) count: 407 at ΓΝ Λαμίας versus 1,991 at the comparable ΓΝ Τρικάλων** — a **4.9× inversion** (local data 2020–May 2026; confirmed against Diavgeia API), robust to any classification ambiguity. The root cause at the hospital is identified: a labour model that outsources cleaning, catering, security, and H/M maintenance to external contractors (€7.34M = 44.9% of the 2024 budget). ΓΝ Τρικάλων also outsources cleaning and security externally — but via competitive tenders that reach a winner (ΓΕΝ ΚΑ ΑΕ for cleaning; bridge contracts for security while tender 26/2024 is contested). Only H/M building maintenance is handled with internal staff at peer hospitals (no CPV 50710000-5 contracts found in peer data 2020–2026). The two hospitals spend almost identically per capita (€244 vs €246), but repeated tender failures at Lamia mean incumbents persist without competition — generating the 4.9× ΚΑΤΑΚΥΡΩΣΗ gap.

---

## Six Documented Findings

**1. Hospital outsourcing incumbency (UNISON).**
The hospital's cleaning contract has been held by UNISON (Metlen Energy & Metals subsidiary, AFM 094081864) since October 2020. A €4.4M international open tender for cleaning services was launched in March 2023 and failed twice — both publications suspended before their bid deadlines by preliminary appeals filed by UNISON and a competitor (IFS IPIROTIKI, GAK 1094/1095, 17 Jul 2023). Four ΕΑΔΗΣΥ consents then authorised a negotiated procedure (Art. 32 § 2γ L.4412/2016). UNISON was formally awarded the negotiated contract in July 2025 (ADA `ΨΠΗΓ4690ΒΜ-ΛΚ8`, €680,859 for 5 months), with the award published four months late in November 2025 — a breach of Art. 3 § 3 L.3861/2010. A parallel competitive tender has a provisional winning bid of **€1.632M** from 3G FACILITIES-SERVICES ΑΕ (ADA `9Μ6Ο4690ΒΜ-ΣΥ6`) — essentially identical to UNISON's negotiated rate, confirming the negotiated price was market-consistent. The documented concerns are: (a) 4-month publication delay; (b) misclassification as Δ.1 (should be Δ.2.4); (c) five-year incumbency during a period when no competitive process produced a winner; (d) total 2025–2026 UNISON commitment: ~€3.2M. Peer comparison: ΓΝ Τρικάλων pays **€800,382/year** (CPV 90911200-8, competitively tendered, ΓΕΝ ΚΑ ΑΕ AFM 094510036, ADA `ΨΕΗΠ46907Φ-Ψ6Φ`) — **2.0× cheaper per bed** for the same service.

**2. Hospital security cost escalation and staffing model.**
Security guard services (CPV 79713000-5) cost ~€361k/year (excl. VAT) during the 2020–2021 bridge contracts (KOLOSSOS SECURITY, 17 FTE). The current contract (ΗΦΑΙΣΤΟΣ ΕΠΕ, AFM 999436428, Heraklion, 2024–2026) runs at ~€529k/year excl. VAT (€656k/yr incl. VAT, €1,968,000 for 3 years) — a **+47% rate increase** from the baseline. The driver is a staffing increase from 17 FTE to 29 FTE (+71%) committed in the 2023 tender specifications (ADA `6ΒΘ84690ΒΜ-464`): 21 concurrent 8-hour shift-slots per day, requiring 28+1 FTE. No clinical or operational justification is documented. At €656k/year and 307 beds, the per-bed cost is **€2,136/bed/year — 2.9× to 4.4× the peer range** (ΓΝ Τρικάλων ~€145k/yr annualised = ~€483/bed, verified from Σύμβ. 40+107+169/2025 bridge contracts via Diavgeia API; ΓΝ Καρδίτσας ~€197k/yr = €741/bed). Note: ΓΝ Τρικάλων's guard count is not available from public data; the per-bed comparison reflects total contract cost. The 2024 tender attracted 2 bidders (ARGOS SECURITY and ΗΦΑΙΣΤΟΣ); the 2021 tender attracted 6 bidders — the market is contested, but the current staffing specification limits effective competition.

**3. Municipality: above-threshold security contracts without competitive tender.**
The municipality directly awarded building security guard contracts above the €30,000 legal threshold in 2022 (two contracts totalling ~€148k: ΚΟΥΤΚΙΑΣ & ΣΙΑ ΕΕ AFM 801089930 in July, ADA `6ΙΕΠΩΛΚ-05Η`; ΚΟΥΤΚΙΑΣ Παναγιώτης AFM 112437726 in August, ADA `ΨΜΛΔΩΛΚ-ΘΙ0`) and 2023 (€74,400, ADA `ΡΒΥΘΩΛΚ-Τ4Κ`). No competitive tender was ever published for building security across the full 2020–2026 dataset. The same family entity also appears in a contract fragmentation pattern (two identical €24,435 same-day security awards, 8 April 2020). Peer municipalities show zero above-threshold direct awards for building security. Total ΚΟΥΤΚΙΑΣ security contracts: ~€222k above threshold, zero competitive process.

**4. Municipality: contract fragmentation by geographic splitting (Δ.Ε.).**
Rolling 60-day window analysis of all 10,388 ΑΝΑΘΕΣΗ decisions identifies 33 suppliers receiving combined awards that exceed the €30k threshold, while each individual award is kept below. The most systematic: **Γ. ΚΑΝΑΒΕΤΑΣ - Χ. ΚΑΡΑΔΗΜΑΣ Ο.Ε.** (AFM 800458127, ΓΕΜΗ 123643354000 — local Lamia electrical materials retailer, verified May 2026; electrical materials, €420k across 34 awards in rolling windows); AFM 112437726 (ΚΟΥΤΚΙΑΣ security, same-day identical awards); AFM 801793425 (single above-threshold award of €37,195). Splitting contracts by Δημοτική Ενότητα to stay below threshold violates Art. 6 § 2 of Law 4412/2016.

**5. ΔΕΥΑ Λαμίας: volume anomaly, but no concentration.**
The utility issues **14.1× more** direct awards than comparable peer utilities (ΔΕΥΑ Τρικάλων 262, Κοζάνη 196, Καρδίτσα 148 → avg 202; ΔΕΥΑ Σερρών excluded as non-comparable). However: all 310 identified suppliers are dispersed (highest-frequency supplier: 17 transactions / ~€14k total over six years); the average transaction value is ~€3,500; dominant categories are maintenance and consumables. The data supports a procurement culture that defaults to direct awards rather than framework agreements, but does **not** support directed spending to specific contractors. ΔΕΥΑ publishes 4,680 ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ decisions — fully comparable with peers, no Diavgeia non-compliance.

**6. Hospital H/M maintenance: sole-bidder incumbency (ΙΝΤΕΡΚΑΤ).**
The €2.46M electromechanical maintenance contract (CPV 50710000-5, ΕΣΗΔΗΣ 345751) was won in November 2024 by ΑΦΟΙ ΠΑΠΑΪΩΑΝΝΟΥ ΑΤΕΒΕ (ΙΝΤΕΡΚΑΤ ΑΕ, AFM 999917261, Γραβιά, Φωκίδα) — the **only remaining bidder**, having held the contract since at least 2018. The 2021 tender (ΕΣΗΔΗΣ 95109) also produced a sole-bidder result. ΙΝΤΕΡΚΑΤ's primary ΓΕΜΗ-registered activities are quarrying and mining (61 registered KADs, zero covering building H/M maintenance — verified May 2026); it qualifies by borrowing technical credentials from subcontractors under Art. 78 L.4412/2016. Two consecutive competitive tenders producing the same sole bidder, across a 7-year incumbency, is the strongest example in the dataset of a recurring high-value contract awarded without effective price competition. Contract value grew from ~€889k/yr (pre-2020) to €820k/yr excl. VAT (€983k incl. VAT, 2024). No peer hospital outsources H/M building maintenance to a single contractor at scale.

---

## What the ΑΝΑΛΗΨΗ Cross-Check Establishes

All 20,283 budget commitment decisions (ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ, 2020–2026) for ΓΝ Λαμίας were hydrated and analysed by KAE expenditure code. The apparent 2.4–5.1× gap between ΑΝΑΛΗΨΗ totals and official payments is fully explained by: (a) centrally-funded payroll commitments (KAE 1312 alone: €24.6M in 2024, exceeding the hospital's entire own-resource payment total); (b) multi-year contract pre-reservations committed in the signing year. **No undisclosed commitments. No anomalous gaps. Financial statements confirmed by this independent source.**

---

## What Is Not Established

The ΑΝΑΘΕΣΗ count divergence at ΓΝ Λαμίας (5,887 vs 220 for ΓΝ Τρικάλων) reflects a classification difference as much as a procurement behaviour difference: ~739 entries are committee steps within competitive processes, coded as ΑΝΑΘΕΣΗ by hospital administrative practice. The defensible direct-award figure is approximately €82.3M (patient-specific and small procurement entries). The defensible comparative metric is the ΚΑΤΑΚΥΡΩΣΗ count (4.9×), not the ΑΝΑΘΕΣΗ count (27×).

---

## Data Sources and Verification Status

| Finding | Primary source | Verification status |
|---------|---------------|-------------------|
| UNISON contract rate | ADA `ΨΠΗΓ4690ΒΜ-ΛΚ8` (Jul 2025 award) | Confirmed — PDF |
| UNISON late publication | Diavgeia issue date vs publication date | Confirmed — 4 months |
| 3G FACILITIES parallel tender | ADA `9Μ6Ο4690ΒΜ-ΣΥ6` (Jul 2025 provisional award) | Confirmed — PDF |
| Trikala cleaning benchmark | ADA `ΨΕΗΠ46907Φ-Ψ6Φ` + `ΨΥΖΚ46907Φ-4ΣΓ` | Confirmed — contract + modification |
| Security escalation rate | ADA `ΩΑΓΕ4690ΒΜ-1Β3` (2021) + `ΕΣΗΔΗΣ 262133` (2024) | Confirmed — board minutes + tender |
| Security per-bed benchmark (Trikala ~€145k/yr) | Σύμβ. 40/2025 `ΨΒΙΒ46907Φ-0Ι2` + 107/2025 `Ρ23Θ46907Φ-45Α` + 169/2025 `ΨΘ7146907Φ-ΧΜΓ` | Confirmed — API €72,502 / 6 months |
| Security per-bed benchmark (Karditsa ~€197k/yr) | ΧΕ PDFs (Karditsa) | Confirmed — local data |
| Karditsa cleaning benchmark (€565,082) | ADA `Ψ6ΟΗ4690ΒΙ-ΝΚΛ`, Σύμβ. 178/2023, 2023-08-03 | Confirmed — CSV + Diavgeia API |
| ΙΝΤΕΡΚΑΤ sole bidder | ADA `6ΕΖΟ4690ΒΜ-ΠΩ5` (2021) + `9ΟΗ84690ΒΜ-Δ5Ε` (2024) | Confirmed — both PDFs |
| ΙΝΤΕΡΚΑΤ ΓΕΜΗ activities | ΓΕΜΗ 14003556000 — fresh lookup May 2026 | Confirmed — 61 KADs, zero H/M |
| ΚΟΥΤΚΙΑΣ above-threshold | ADAs `6ΙΕΠΩΛΚ-05Η`, `ΨΜΛΔΩΛΚ-ΘΙ0`, `ΡΒΥΘΩΛΚ-Τ4Κ` | Confirmed — Diavgeia decisions |
| ΔΕΥΑ volume anomaly | org=50304 decisions.csv (2,840 ΑΝΑΘΕΣΗ) | Confirmed — local data |
| ΚΑΤΑΚΥΡΩΣΗ 4.9× | org=99221946 decisions.csv (1,991) vs org=99221923 (407) | Confirmed — local data, May 2026 |
| ΑΝΑΛΗΨΗ financial cross-check | ADA `9ΤΥΕ4690ΒΜ-69Α` (2024 budget execution) | Confirmed — KAE breakdown |

*Full analysis: `reports/narrative_analysis.md` · Dashboard: `reports/overview.html`*
*Git branch: `claude/modest-mclaren-aeb419` · Worktree: `.claude/worktrees/modest-mclaren-aeb419`*
