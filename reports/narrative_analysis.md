# Procurement Transparency in Lamia: What the Data Shows
*Analysis of Diavgeia public procurement records, 2020–2026*
*Data: Diavgeia OpenData API · Coverage: search exports + targeted detail hydration*
*Revised: May 2026 — includes data quality audit findings*

---

## Executive Summary

Across three distinct public entities in Lamia — the municipality, the general hospital, and the water utility — a consistent pattern emerges: direct contract awards (ΑΝΑΘΕΣΗ) are issued significantly more often than in comparable institutions elsewhere. The hospital shows the starkest divergence: 5,887 ΑΝΑΘΕΣΗ entries versus 187 for the comparable peer hospital in Trikala.

**However, a detailed audit of the underlying data substantially changes the interpretation of the financial figures.** The previously reported figure of €215.5M in "direct award spending" at ΓΝ Λαμίας contains ~€132.8M derived from committee decisions within *competitive* tender processes, not from direct awards. The most reliable financial signal is the ΚΑΤΑΚΥΡΩΣΗ count — ΓΝ Τρικάλων runs competitive tenders that result in formal awards (ΚΑΤΑΚΥΡΩΣΗ) at 4.5× the rate of ΓΝ Λαμίας (1,841 vs 407). This inversion is the cleanest finding in the dataset.

---

## Part 1 — What the Data Shows

### 1.1 The Municipality

Greek municipalities are required to publish all procurement decisions on the Diavgeia transparency portal. A decision classified as ΑΝΑΘΕΣΗ (direct award) means the contract was assigned to a supplier without a competitive tender. The legal threshold below which municipalities may award directly is approximately €30,000 for goods and supplies.

| Municipality | Direct awards (ΑΝΑΘΕΣΗ) | Rate | Direct award spend | Competitive spend |
|---|---|---|---|---|
| **Lamia** | **10,388** | **16.1%** | €25.6M | €34.3M |
| Trikala | 6,991 | 11.5% | €24.1M | €0.4M |
| Serres | 1,586 | 2.6% | €23.1M | €0.4M |
| Karditsa | 2,709 | 4.6% | — | — |

*Note: Karditsa municipality codes competitive works as ΑΝΑΘΕΣΗ (avg €302k/decision vs €11k for Lamia/Trikala) — amounts are not comparable.*

Lamia issues the most direct award decisions by count (10,388) and the highest rate (16.1% of all published decisions), compared to 11.5% for Trikala and 2.6–4.6% for the others. The municipality also has €34.3M in competitive procurement — the most of any peer — making the picture more nuanced than the hospital or utility.

**Municipality summary:** High absolute count of direct awards (10,388 over 6 years ≈ 1,700/year) warrants examination of whether these are genuinely below-threshold or represent fragmented larger contracts. The average transaction value for ΑΝΑΘΕΣΗ decisions with amounts is €11k, consistent with the legal direct-award threshold.

---

### 1.2 The Hospital (ΓΝ Λαμίας)

**Note on data quality:** A classification audit of ΓΝ Λαμίας's 5,887 ΑΝΑΘΕΣΗ entries found the following breakdown:

| Category | Decisions | % | Associated amounts |
|---|---|---|---|
| Patient-specific & small procurement | 5,081 | 86.3% | €82.3M (in 1,899 decisions) |
| **Committee steps within competitive tenders** | **739** | **12.6%** | **€132.8M (in 614 decisions)** |
| Committee/board approvals | 48 | 0.8% | €0.2M |
| Explicitly labelled direct awards | 15 | 0.3% | €0.1M |

The 739 "competitive tender steps" are decisions such as *"Decision on the evaluation committee's report for the International Open Electronic Tender"* — they carry the estimated contract value but are **not direct awards**. They are procedural documentation of competitive processes in progress.

This means the €215.5M figure reported in earlier versions of this analysis **overstates** direct award spending. The €132.8M component reflects contract values of competitive tenders under evaluation, not money awarded without competition.

**What can be said with confidence:**

| Hospital | ΑΝΑΘΕΣΗ (Δ.1) | ΚΑΤΑΚΥΡΩΣΗ (competitive) | Ratio |
|---|---|---|---|
| **ΓΝ Λαμίας** | **5,887** (11.6%) | **407** | **14.5 : 1** |
| ΓΝ Τρικάλων | 187 (0.3%) | 1,841 | 0.1 : 1 |
| ΓΝ Σερρών | 184 (0.7%) | 82 | 2.2 : 1 |

The **ΚΑΤΑΚΥΡΩΣΗ count is the more reliable metric**: it measures formal competitive award decisions using an unambiguous decision type, not susceptible to the classification ambiguity affecting ΑΝΑΘΕΣΗ. ΓΝ Τρικάλων completes competitive procurement awards **4.5× more often** than ΓΝ Λαμίας, despite being a comparable hospital serving a similar population.

On amounts: For the 937 ΑΝΑΘΕΣΗ decisions at ΓΝ Λαμίας that have both amounts *and* supplier identifiers, total spend is €12.25M at an average of €13,100 — consistent with below-threshold direct procurement. For the 1,628 decisions with amounts but no supplier identifier (sourced from subject-text extraction and hydrated fields), amounts total €203M but cannot be attributed to specific suppliers or verified as procurement rather than procedural entries.

**Revised hospital summary:** ΓΝ Λαμίας runs competitive procurement (ΚΑΤΑΚΥΡΩΣΗ) at one-quarter the rate of ΓΝ Τρικάλων. This is a robust finding. The financial scale of below-threshold direct awards is likely significant but the €215.5M headline figure requires cross-validation against the hospital's published financial accounts before being stated as confirmed.

---

### 1.3 The Water Utility (ΔΕΥΑ Λαμίας)

| ΔΕΥΑ | Direct awards | Rate | Direct spend | Competitive spend | Direct % of spend |
|---|---|---|---|---|---|
| **ΔΕΥΑ Λαμίας** | **2,840** | **11.6%** | €10.1M | €0.74M | **93.1%** |
| ΔΕΥΑ Τρικάλων | 262 | 1.8% | €12.3M† | — | — |
| ΔΕΥΑ Κοζάνης | 196 | 1.4% | €30.7M | — | — |
| ΔΕΥΑ Σερρών | 1,180 | 7.6% | — | — | — |
| ΔΕΥΑ Καρδίτσας | 148 | 1.0% | €23.7M | — | — |

*†ΔΕΥΑ Τρικάλων includes €5.6M spike in 2020 (likely one large project)*

ΔΕΥΑ Λαμίας's **average direct award transaction is approximately €3,500** — well within the legal direct-award threshold. The count anomaly (6.4× the peer average) is therefore about **frequency of small purchases** rather than above-threshold evasion of competition. The utility channels 93.1% of its identifiable procurement spend through direct awards and runs virtually no competitive tenders of substance (€0.74M over six years).

**ΔΕΥΑ per-capita context:** From 2023 onward, ΔΕΥΑ Λαμίας records only €9–10/capita/year in procurement spend (€670k–750k/year for a water and sewerage utility serving 75,000 people). Peer utilities: ΔΕΥΑ Κοζάνης €50–177/capita, ΔΕΥΑ Καρδίτσας €66–109/capita and growing.

---

## Part 2 — What It Means

### 2.1 The Coding Problem and What Survives It

The ΑΝΑΘΕΣΗ count anomaly at ΓΝ Λαμίας (5,887 vs 187) partially reflects a **classification practice** where competitive tender procedural decisions are coded under the ΑΝΑΘΕΣΗ decision type. Removing the 739 identified competitive-process entries, ΓΝ Λαμίας would still have ~5,148 ΑΝΑΘΕΣΗ versus ΓΝ Τρικάλων's 187. The divergence is robust to this correction.

The ΚΑΤΑΚΥΡΩΣΗ inversion (407 Lamia vs 1,841 Trikala) uses a more precise decision type and tells the same story unambiguously: ΓΝ Τρικάλων overwhelmingly chooses to run competitive processes to formal award; ΓΝ Λαμίας does not.

### 2.2 Three Entities, One Pattern

The same signal — elevated direct award frequency — appears independently in three separate organisations sharing only a common location. Each has its own legal personality, governing board, management. That all three independently display elevated direct award rates suggests the pattern reflects something about the local procurement culture or oversight environment rather than any particular operational constraint.

### 2.3 Possible Explanations

**Operational necessity.** A hospital or utility may legitimately need to make many urgent, below-threshold purchases. The patient-specific procurement at ΓΝ Λαμίας (surgical materials ordered per operation) is a documented operational practice. However, ΓΝ Τρικάλων faces identical operational pressures and runs 4.5× more competitive tenders.

**Administrative preference.** Some procurement officers and institutional cultures prefer the speed and simplicity of direct award over the administrative burden of competitive tendering.

**Contract fragmentation.** A large purchase split into tranches, each below the competitive threshold. For ΔΕΥΑ Λαμίας (avg €3,500/transaction), the volume — roughly one direct-award transaction every working day — is more consistent with fragmentation than with genuine single-item emergencies.

**Deliberate avoidance of competition.** At the extreme end: directing public spending to selected suppliers outside the scrutiny of competitive process. This cannot be asserted from count data alone and requires supplier concentration analysis with complete supplier identifiers.

### 2.4 What Financial Cross-Validation Would Look Like

The hospital's published financial statements (ΙΣΟΛΟΓΙΣΜΟΣ – ΑΠΟΛΟΓΙΣΜΟΣ) are present on Diavgeia but contain no structured numerical data — they are approval decisions referencing attached PDFs. Cross-validation requires:

1. Obtaining the annual financial accounts from the hospital's audited reports
2. Comparing the hospital's total procurement expenditure line with the Diavgeia-sourced amounts
3. For ΔΕΥΑ Λαμίας: comparing with the utility's published annual accounts (required for entities of this type under Greek utility regulation)

---

## Part 3 — Conclusions

**What is established (count-based, robust):**

1. ΓΝ Λαμίας issues procurement entries under ΑΝΑΘΕΣΗ at a rate 31× higher than ΓΝ Τρικάλων. Even accounting for ~739 competitive-process administrative entries in that count, the divergence is ~27× after correction.

2. ΓΝ Τρικάλων completes competitive procurement (ΚΑΤΑΚΥΡΩΣΗ) 4.5× more often than ΓΝ Λαμίας. This is the cleanest finding in the dataset.

3. ΔΕΥΑ Λαμίας issues 6.4× more direct award transactions than the peer average, with 93% of identifiable spend going through this channel. The average transaction value (€3,500) is within the legal threshold — the concern is volume and the near-total absence of competitive procurement.

4. All three Lamia entities independently show elevated direct award frequency, suggesting systemic pattern rather than institution-specific circumstance.

**What requires revision from earlier drafts:**

- The €215.5M headline figure should not be presented as confirmed direct award spending. It contains ~€132.8M from competitive tender procedural entries. The more defensible figure for identified below-threshold awards is approximately €82.3M (from the 86.3% of ΑΝΑΘΕΣΗ that are patient-specific or small procurement decisions).

- Amount-based supplier concentration analysis is not currently possible for ΓΝ Λαμίας: 63.5% of decisions with amounts (€203M) have no supplier identifier in the Diavgeia structured fields.

**Supplier analysis — completed (May 2026):**

*ΓΝ Λαμίας ΚΑΤΑΚΥΡΩΣΗ suppliers (55 unique, 53 identified via ΓΕΜΗ):*
All 53 are legitimate medical device, pharmaceutical, diagnostic, or equipment companies. The most frequent recipients of competitive awards — Abbott Laboratories, bioMérieux, Delta Medical, ΑΝΤΙΣΕΛ — are national and multinational healthcare suppliers operating across many Greek hospitals. No anomalous or non-medical companies appear in the competitive award channel.

*Dual-channel overlap:* 42 of 55 ΚΑΤΑΚΥΡΩΣΗ suppliers (76%) also appear in ΑΝΑΘΕΣΗ at ΓΝ Λαμίας. This is expected: hospitals maintain ongoing relationships with key medical suppliers across both competitive and urgent/small procurement channels. The ΑΝΑΘΕΣΗ amounts for these overlapping suppliers average €13,100 per transaction — consistent with the below-threshold range. No evidence of inflated pricing in the direct channel for competitive award recipients.

*ΔΕΥΑ Λαμίας — no concentration found:* 310 unique suppliers across 2,840 direct award transactions. The highest-frequency supplier (17 transactions) has a total spend of ~€14k over six years. No supplier dominates the channel. The pattern is consistent with genuine dispersed operational procurement (maintenance, small parts, consumables), not directed spending.

*ΑΝΑΘΕΣΗ classification — additional categories identified:*
Beyond the 739 competitive-tender procedural steps identified earlier, two further categories inflate the ΑΝΑΘΕΣΗ figures:
- **Negotiated contracts coded as ΑΝΑΘΕΣΗ:** UNISON Facility Services (cleaning) received 6 contracts in 2025–2026 totalling €3.24M, awarded under "negotiated procedure without prior publication" (Art. 32 § 2(c) of Law 4412/2016). These are large-value service contracts with ΕΑΔΗΣΥ oversight — legally distinct from below-threshold direct awards but classified identically in Diavgeia.
- **Option exercises coded as ΑΝΑΘΕΣΗ:** Several ΑΝΑΘΕΣΗ entries are formal exercises of contractual option rights on competitively-awarded contracts (e.g., ΠΙΕΤΡΗΣ catering €660k, LINDE medical gases extensions). These are legitimate but inflate the ΑΝΑΘΕΣΗ count and amounts.

*Key finding on UNISON:* ΓΝ Λαμίας has run no competitive tender for hospital-wide cleaning services at scale. A 2023 competitive award for cleaning (ΚΑΤΑΚΥΡΩΣΗ, €81k) appears to have covered a limited scope. From June 2025, the hospital has used the Art. 32§2γ emergency pathway repeatedly — awarding UNISON a 5-month contract (€680k), extending it twice, and awarding a further 5-month extension (€773k) in April 2026. Using emergency procedures for a recurring, foreseeable core service raises a compliance question independent of supplier identity.

**What requires further investigation:**

- **UNISON compliance:** Whether the repeated use of Art. 32§2γ for hospital cleaning is justified or whether the hospital had sufficient time to run a competitive tender between contract cycles. The decision `ΨΜΧΥ4690ΒΜ-75Β` (ΕΑΔΗΣΥ consent) and `9Π3Γ4690ΒΜ-ΡΗΠ` (second extension with ΕΑΔΗΣΥ referral) are the key documents.

- **ΚΑΤΑΚΥΡΩΣΗ amounts at ΓΝ Λαμίας:** The 407 competitive awards have amounts for 335 decisions. What is the total competitive procurement spend? Is it proportionate to the hospital's scale?

- **Cross-validation against financial accounts:** The ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ (budget commitment) entries at ΓΝ Λαμίας (20,286 decisions) should provide a cross-check of total procurement commitments independent of the ΑΝΑΘΕΣΗ classification.

- **ΔΕΥΑ Σερρών:** Also shows high direct award counts (1,180, 7.6%) — warrants equivalent scrutiny.

**Caveats:**

- Lamia municipality data for 2022–2024 is likely undercounted (API cap of 500 decisions/month — windowed re-fetch pending). True volumes are probably ~40–50% higher.
- ΓΝ Καρδίτσας and ΓΝ Κοζάνης use Β-type decision codes for procurement — not directly comparable with ΓΝ Λαμίας on ΑΝΑΘΕΣΗ counts.
- All amounts are dependent on quality of original Diavgeia data entry and completeness of hydration.

---

*Data sources: Diavgeia OpenData API (diavgeia.gov.gr) · Period: January 2020 – May 2026*
*Processing pipeline: windowed search-export collection + targeted detail hydration (types Δ.1, Δ.2.2, Γ.3.4)*
*Data quality audit: May 2026 — classification of ΑΝΑΘΕΣΗ subjects, supplier coverage analysis*
*Code and data: available in this repository under `scripts/` and `data/normalized/`*
