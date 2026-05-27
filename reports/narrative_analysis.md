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
| ΔΕΥΑ Καρδίτσας | 148 | 1.0% | €23.7M | — | — |
| ΔΕΥΑ Σερρών | ~~1,180~~ | ~~7.6%~~ | — | — | — |

*†ΔΕΥΑ Τρικάλων includes €5.6M spike in 2020 (likely one large project)*
*ΔΕΥΑ Σερρών: 90.8% of ΑΝΑΘΕΣΗ are generic board resolutions (Απόφαση ΔΣ/Προέδρου) — not comparable, see §2.1*

ΔΕΥΑ Λαμίας's **average direct award transaction is approximately €3,500** — well within the legal direct-award threshold. The count anomaly is now **4.9× the corrected peer average** (excluding ΔΕΥΑ Σερρών) rather than 6.4×. The utility channels 93.1% of its identifiable procurement spend through direct awards and runs virtually no competitive tenders of substance (€0.74M over six years).

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

### 2.4 Financial Cross-Validation — Completed

The hospital's 2024 budget execution report (published on Diavgeia as ADA `9ΤΥΕ4690ΒΜ-69Α`, approved 30 June 2025) provides machine-readable procurement totals. Combined with ΚΑΤΑΚΥΡΩΣΗ amounts sourced from Diavgeia decision records, this enables a direct cross-check.

**ΓΝ Λαμίας — 2024 procurement by category (from official budget execution report):**

| Category | Amount | Notes |
|----------|--------|-------|
| Φάρμακα (drugs) | €1,703,137 | 66.6% absorbed only — remainder via ΕΚΑΠΥ central procurement |
| Υγειονομικό υλικό (medical consumables) | €2,805,230 | |
| Ορθοπεδικό υλικό | €1,068,996 | |
| Χημικά–αντιδραστήρια | €1,432,384 | |
| Μισθοδοσία (payroll) | €3,064,252 | |
| Outsourcing (cleaning, catering, security) | €7,342,516 | 44.9% of total |
| Λοιπά | €864,766 | |
| **ΣΥΝΟΛΟ** | **€18,281,282** | |

**Multi-year procurement totals (from same report, Table 2):**

| Year | Total (financial stmt) | ΚΑΤΑΚΥΡΩΣΗ amounts (Diavgeia) | ΚΑΤΑΚΥΡΩΣΗ % of total |
|------|----------------------|-------------------------------|----------------------|
| 2019 | €16,390,000 | — | — |
| 2020 | €18,594,371 | €3,791,675 | **20.4%** |
| 2021 | €26,703,546 | €1,236,028 | 4.6%* |
| 2022 | €28,274,078 | €7,033,382 | **24.9%** |
| 2023 | €26,467,809 | €11,693,651 | **44.2%** |
| 2024 | €18,281,282 | €13,437,060 | **73.5%** |

*\*2021 low figure likely reflects incomplete hydration coverage for that year, not a genuine procurement pattern.*

**Key findings from cross-validation:**

1. **ΑΝΑΘΕΣΗ amounts from Diavgeia are wildly inconsistent with actual procurement.** Diavgeia-sourced ΑΝΑΘΕΣΗ amounts at ΓΝ Λαμίας range from €33M–€51M per year, versus actual total hospital procurement of €18M–€28M per year. This confirms the classification problem: the ΑΝΑΘΕΣΗ decision type contains large amounts from competitive tender *procedural* entries, not actual direct award spending.

2. **ΚΑΤΑΚΥΡΩΣΗ by value is increasingly dominant.** Formal competitive awards (ΚΑΤΑΚΥΡΩΣΗ) as a share of total procurement grew from 20% in 2020 to 73.5% in 2024. The hospital runs fewer competitive *processes* than ΓΝ Τρικάλων (407 vs 1,841 awards 2020–2026) but those it does run account for the majority of procurement spend by value in recent years.

3. **2024 is understated** by approximately €8.9M due to ΕΚΑΠΥ central pharmaceutical procurement (drugs purchased centrally and delivered directly, not appearing in the hospital's own ΚΑΤΑΚΥΡΩΣΗ records from 2022 onward).

4. **Outstanding unpaid obligations (end of 2024):** €19.7M — of which pharmaceuticals €8.0M and services/materials €8.2M. This is a persistent liquidity issue documented in the official accounts.

5. **Outsourcing (cleaning, catering, security) = 44.9% of hospital spending** (€7.3M of €18.3M in 2024). This is the category within which the UNISON cleaning contract anomaly (§3, ΚΑΤΑΚΥΡΩΣΗ findings) sits — making the absence of competitive tendering for cleaning particularly significant given the scale.

---

## Part 3 — Conclusions

**What is established (count-based, robust):**

1. ΓΝ Λαμίας issues procurement entries under ΑΝΑΘΕΣΗ at a rate 31× higher than ΓΝ Τρικάλων. Even accounting for ~739 competitive-process administrative entries in that count, the divergence is ~27× after correction.

2. ΓΝ Τρικάλων completes competitive procurement (ΚΑΤΑΚΥΡΩΣΗ) 4.5× more often than ΓΝ Λαμίας. This is the cleanest finding in the dataset.

3. ΔΕΥΑ Λαμίας issues 4.9× more direct award transactions than the comparable peer average (ΔΕΥΑ Τρικάλων, Καρδίτσας, Κοζάνης — excluding ΔΕΥΑ Σερρών whose ΑΝΑΘΕΣΗ count reflects board resolutions, not procurement awards), with 93% of identifiable spend going through this channel. The average transaction value (€3,500) is within the legal threshold — the concern is volume and the near-total absence of competitive procurement.

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

*Key finding on UNISON (verified against Diavgeia API):* ΓΝ Λαμίας has run no competitive tender for hospital-wide cleaning services at scale. A 2023 competitive award for cleaning (ΚΑΤΑΚΥΡΩΣΗ, €81k) covered a limited scope. The large-scale cleaning contract history:

| ADA | Date | Amount | Decision |
|-----|------|--------|----------|
| ΨΠΗΓ4690ΒΜ-ΛΚ8 | 2025-07-02 | €680,859 | Award of cleaning services (Δ.1) |
| ΨΜΧΥ4690ΒΜ-75Β | 2025-12-04 | €680,859 | ΕΑΔΗΣΥ consent for Art.32§2γ procedure |
| 9ΗΡΣ4690ΒΜ-ΕΕ3 | 2026-02-25 | €482,701 | Contract modification |
| 96Η04690ΒΜ-ΠΔ2 | 2026-02-27 | €483,309 | Cleaning services continuation |
| 9Π3Γ4690ΒΜ-ΡΗΠ | 2026-05-25 | €772,767 | Second 5-month extension via negotiation |

Two additional findings from Diavgeia detail records:
1. **Late publication:** ADA `ΨΠΗΓ4690ΒΜ-ΛΚ8` was issued July 2, 2025 but published on Diavgeia November 2, 2025 — a 4-month delay. Greek law (Art. 3 § 3 of Law 3861/2010) requires publication within 15 days.
2. **Sequence anomaly:** The ΕΑΔΗΣΥ consent (`ΨΜΧΥ4690ΒΜ-75Β`, December 2025) post-dates the initial award (`ΨΠΗΓ4690ΒΜ-ΛΚ8`, July 2025) by 5 months. For Art.32§2γ procedures, ΕΑΔΗΣΥ consent should precede the award. The December 2025 consent appears to authorise a *subsequent* 5-month extension, but the July 2025 award lacks documented prior regulatory authorisation in the Diavgeia record.

Using emergency procedures for a recurring, foreseeable core service raises a compliance question independent of supplier identity. The ADA references above provide direct entry points for ΕΑΔΗΣΥ or Ελεγκτικό Συνέδριο review.

**What requires further investigation:**

- **UNISON compliance:** Whether the repeated use of Art. 32§2γ for hospital cleaning is justified or whether the hospital had sufficient time to run a competitive tender between contract cycles. The decision `ΨΜΧΥ4690ΒΜ-75Β` (ΕΑΔΗΣΥ consent) and `9Π3Γ4690ΒΜ-ΡΗΠ` (second extension with ΕΑΔΗΣΥ referral) are the key documents.

- **ΚΑΤΑΚΥΡΩΣΗ amounts at ΓΝ Λαμίας — answered:** 335 decisions with amounts, total €45.26M (2020–2026), growing from €3.79M (2020) to €13.44M (2024). Cross-validation against official budget execution confirms ΚΑΤΑΚΥΡΩΣΗ = 73.5% of total procurement spend in 2024. Proportionate to hospital scale.

- **ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ cross-check:** The 20,286 budget commitment decisions at ΓΝ Λαμίας could provide an independent cross-check of total procurement commitments, but hydrating this volume (~2–3 hours pipeline run) is lower priority now that financial statement cross-validation is complete.

- **ΔΕΥΑ Σερρών — coding artifact, not comparable:** A full inspection of ΔΕΥΑ Σερρών's 1,180 ΑΝΑΘΕΣΗ decisions shows 90.8% are `Απόφαση ΔΣ/Προέδρου` board and chairman resolutions (budget votes, HR decisions, tender approvals, project extensions) — not specific procurement award decisions. Amount coverage is 0.4% (5 decisions); supplier identifier coverage is 0%. ΔΕΥΑ Σερρών has 77 ΠΕΡΙΛΗΨΗ ΔΙΑΚΗΡΥΞΗΣ (tender notices) but only 1 ΚΑΤΑΚΥΡΩΣΗ, suggesting competitive awards are coded as board resolutions (ΑΝΑΘΕΣΗ type) rather than as ΚΑΤΑΚΥΡΩΣΗ. The count divergence between ΔΕΥΑ Σερρών and peers reflects administrative coding practice, not elevated direct procurement. **ΔΕΥΑ Σερρών cannot be compared with ΔΕΥΑ Λαμίας on the ΑΝΑΘΕΣΗ metric.** The appropriate comparison set for ΔΕΥΑ Λαμίας remains ΔΕΥΑ Τρικάλων, Καρδίτσας, and Κοζάνης.

**Municipality supplier analysis — completed (May 2026):**

*Coverage:* 39.2% of municipality ΑΝΑΘΕΣΗ decisions have a supplier tax identifier (4,066 of 10,388). Total identified spend: €24.7M of the stated €25.6M — once a single data entry error is excluded (ADA `6ΗΞ4ΩΛΚ-20Ρ`, where the lawyer's AFM `130561769` was entered as the amount field, creating a spurious €130.5M entry).

*Supplier concentration:* 839 unique suppliers. Top-5 hold 19.6% of identified spend; top-10 hold 26.8%. No dominant supplier. The profile is consistent with dispersed operational procurement across many small local businesses and individuals.

*Transaction composition:* 63.7% maintenance and repair, 15.5% supplies — consistent with genuine below-threshold operational spending. 41.1% of transactions are under €1,000; 73.6% are under €5,000.

*Key identified suppliers (via ΓΕМΗ):* All local or regional businesses — electrical contractor (25248544, Δομοκός, 79tx at avg €4,486), HVAC engineer (112428914, Λαμία, 62tx), playground equipment manufacturer (103100437, Λαγύνα, 11tx), concrete supplier (094082639 ΑΤΛΑΣ ΜΠΕΤΟΝ, Λαμία, 10tx), state postal service ΕΛΤΑ (094026421, 6tx — legally required direct award to monopoly). The ΕΛΤΑ entry (€332k total) is normal and legally mandated.

*ΝΤΟΥΒΑΣ family relationship:* Two entities sharing a surname — ΣΤ. ΝΤΟΥΒΑΣ & ΣΙΑ Ο.Ε. (fire prevention, 25tx, €319k) and ΝΤΟΥΒΑΣ ΒΑΣΙΛΕΙΟΣ & ΣΙΑ Ε.Ε. (sports field maintenance, 14tx, €220k) — total €540k combined over six years. Different legal entities, different services, different geographic focus. At this scale over six years, not a concentration anomaly.

**Threshold-ceiling pattern:** The data shows awareness of the legal direct-award ceiling (€30,000 net = €37,200 gross with 24% VAT under Law 4412/2016). Subject-text extraction from the full search export corpus identifies a cluster of decisions where the stated budget is exactly €37,200, with an abrupt drop-off above that amount. The pattern is consistent with systematic alignment to the maximum permitted direct-award amount. *Note: amount coverage in the Diavgeia structured data for this org is low (~0.1% of decisions), so the exact count of ceiling-aligned decisions cannot be stated with precision from available data — the observation is directional rather than a counted fact.*

**Triangulated above-threshold direct awards (Diavgeia API confirmed):**

Each decision below was fetched from the Diavgeia API. None has a `directAwardJustification` or `directAwardType` field — no emergency exception was claimed. All are coded `documentType: ΠΡΑΞΗ` (administrative act = direct award). Confirmed by cross-checking the municipality's full competitive tender record (ΠΕΡΙΛΗΨΗ ΔΙΑΚΗΡΥΞΗΣ, ΚΑΤΑΚΥΡΩΣΗ types): **zero competitive tenders were ever published for building security or fire detection maintenance** in the 2020–2026 dataset.

| ADA | Date | Supplier (AFM) | Amount | Service | Status |
|-----|------|----------------|--------|---------|--------|
| ΨΜΛΔΩΛΚ-ΘΙ0 | 2022-08 | ΚΟΥΤΚΙΑΣ Παν. (112437726) | €73,999 | Building security services | AFM absent from Diavgeia structured data; confirmed as ΚΟΥΤΚΙΑΣ via decision PDF. Second award same year. |
| 6ΙΕΠΩΛΚ-05Η | 2022-07 | ΚΟΥΤΚΙΑΣ & ΣΙΑ ΕΕ (801089930) | €73,999 | Building security services | Same service, same year → combined €148k |
| ΡΒΥΘΩΛΚ-Τ4Κ | 2023-07 | ΚΟΥΤΚΙΑΣ Παν. (112437726) | €74,400 | Building security services | Individual, same family |
| Ψ2ΡΗΩΛΚ-8ΓΠ | 2022-02 | ΖΗΚΑ & ΚΑΙΛΑΣ ΕΕ (099509954) | €74,400 | EU social program management | CPV 85312320-8; see note below |
| 6ΕΔ0ΩΛΚ-0ΤΖ | 2024-10 | ΖΗΚΑ & ΚΑΙΛΑΣ ΕΕ (099509954) | €74,400 | Community centre publicity & evaluation | Same CPV misapplied |

*Exemptions (not counted): legal fees (98Θ3ΩΛΚ-Κ8Λ, €127k) — legal representation is exempt; ΕΛΤΑ postal fees (9Ο6ΣΩΛΚ-ΠΜ3, €68k) — ΕΛΤΑ has Universal Service Obligation, direct award legally required.*

**Finding 1 — Fire detection maintenance (ΑΝΑΣΤΑΣΙΟΥ): ~~€482k~~ DATA ENTRY ERROR — corrected to €4,824**

PDF verification of ADA `ΡΩΖΡΩΛΚ-Ω5Σ` (August 2025) showed the actual awarded amounts are €2,418 + €1,472 net (= €4,824 gross, ~€3,823.44 net total). The Diavgeia structured `awardAmount` field contained €482,360 — a typographic error in the original data entry, likely transposing digits from a different field. Two ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ (budget commitment) entries for the same decision total €4,823.60 net, exactly confirming the PDF figures. **This decision is within the legal direct-award threshold and does not constitute a procurement irregularity.** It has been removed from the above-threshold findings table.

ΑΝΑΣΤΑΣΙΟΥ's full award history at the municipality: €404 (2021), €4,359 (2022), €979 (2024), €4,824 (2025) — all well within the €30,000 net threshold. The ΓΕΜΗ-registered activity "electronic lab and antenna installation" is consistent with these small fire/security detection maintenance jobs. **No procurement anomaly identified for this contractor.**

*Residual observation (not a finding):* The municipality runs competitive tenders for fire detection *installation* (ΚΑΤΑΚΥΡΩΣΗ entries found: 2022 nurseries, 2023 technical study) but awards maintenance via ΑΝΑΘΕΣΗ. Since all maintenance awards are under threshold, this is legally compliant operational practice — not an irregularity.

**Finding 2 — Building security (ΚΟΥΤΚΙΑΣ family, €222k+):**
Security guard services for municipal buildings were directly awarded in consecutive years. In 2022 alone, two contracts totalling ~€148k were issued: one to the company (ΚΟΥΤΚΙΑΣ & ΣΙΑ ΕΕ, 801089930, July, ADA `6ΙΕΠΩΛΚ-05Η`) and one to the same individual (ΚΟΥΤΚΙΑΣ Παναγιώτης, AFM 112437726, August, ADA `ΨΜΛΔΩΛΚ-ΘΙ0`) — the August decision had no supplier AFM in the Diavgeia structured data, confirmed via decision PDF. In 2023 the contract went to the same-surname individual (ΚΟΥΤΚΙΑΣ Παναγιώτης, 112437726). The same entities also appear for small amounts at ΔΕΥΑ Λαμίας (€2,349) and Δήμος Στυλίδας (€1.2–3k), confirming they are local contractors — not phantom entities. The security guard service pre-2022 was within threshold (€24–26k/year). The jump to €74k in 2022 without a competitive tender is a procedural issue independent of who the supplier is.

*Peer comparison:* Δήμος Τρικκαίων and Δήμος Καρδίτσας show zero above-threshold security direct awards. Δήμος Κοζάνης has three security direct awards — all for specialized facilities (domestic violence shelters), all within threshold (€12–16k). No peer municipality shows building-wide security contracts above threshold issued as direct awards.

**Finding 3 — ΖΗΚΑ & ΚΑΙΛΑΣ (CPV ambiguity):**
Both awards use CPV `85312320-8` "Social counselling services" — a category that benefits from higher thresholds under Annex XIV of Directive 2014/24/EU (Art. 107 of Law 4412/2016). However, the 2024 award (`6ΕΔ0ΩΛΚ-0ΤΖ`) is explicitly for "publicity and evaluation services for the Community Centre" — not social counselling — and the same CPV code appears misapplied. Whether the 2022 social programme management service genuinely qualifies for the Annex XIV light-touch regime requires legal assessment outside the scope of this data analysis. The CPV mismatch on the 2024 contract is a separate, documentable issue.

**Municipality summary:** The overall supplier picture does not show directed spending or supplier capture. The concerns are specific: (a) building security services were directly awarded above threshold in 2022 (€148k combined, two contracts for the same service in the same year, one with no supplier identifier in the structured record — confirmed via PDF as ΚΟΥΤΚΙΑΣ Παναγιώτης AFM 112437726) and 2023 (€74.4k) without competitive tendering; (b) ΖΗΚΑ & ΚΑΙΛΑΣ received above-threshold direct awards where the CPV classification is questionable, particularly the 2024 contract; (c) a threshold-ceiling pattern in stated award amounts (abrupt drop-off above €37,200) indicates systematic awareness of the legal limit; (d) two data entry errors in amount fields inflate headline totals. Two previously reported findings have been removed after PDF verification: the €482,360 fire detection award was a Diavgeia data entry error (actual: €4,824), and the €91,460 BRINX protective clothing entry was a competitive open tender result mis-coded as ΑΝΑΘΕΣΗ (four bidders competed; BRINX won two lots).

---

**Caveats:**

- Δήμος Λαμίας 2022–2024 data is **complete**: cross-check of monthly_summary vs decisions.csv shows 100% match for all 7 years. Earlier concern about API cap undercounting was resolved by the pipeline. Total 64,334 decisions, 10,388 ΑΝΑΘΕΣΗ confirmed.
- ΓΝ Καρδίτσας and ΓΝ Κοζάνης use Β-type decision codes for procurement — not directly comparable with ΓΝ Λαμίας on ΑΝΑΘΕΣΗ counts.
- All amounts are dependent on quality of original Diavgeia data entry and completeness of hydration.

---

---

## Open Questions & Verification Notes

*These items are not required for regulatory referral but would strengthen the record further.*

### Confirmed but not fully resolved

- **ΖΗΚΑ & ΚΑΙΛΑΣ — Annex XIV applicability:** CPV 85312320-8 "Social counselling services" potentially triggers the Annex XIV light-touch regime (Art. 107 of Law 4412/2016, threshold €750k for full competitive procedure). However: (a) direct award is still capped at €30k net under Art. 118 even for Annex XIV services; (b) the 2024 contract (€74.4k, "publicity and evaluation for the Community Centre") clearly does not qualify as social counselling. Requires legal assessment for the 2022 contracts; the 2024 award stands regardless.

- **912ΡΩΛΚ-ΨΧΠ (€91.4k, BRINX ΑΕ, protective clothing 2020):** PDF extraction showed multiple lot amounts (€131,774 total across lots, including €61,780, €17,573, €18,899 sub-items) and a council minutes header ("ΑΠΟΦΑΣΗ 458/2020, Απόσπασμα από το πρα…"). This suggests the decision may be a municipal council ratification of a competitive tender result (ΚΑΤΑΚΥΡΩΣΗ mis-coded as ΑΝΑΘΕΣΗ) rather than a direct award. The ΑΠΟΦΑΣΙΖΟΥΜΕ section was not fully extracted. **Recommend checking whether a prior ΚΑΤΑΚΥΡΩΣΗ exists for CPV 35113400 (protective clothing) 2020 for Δήμος Λαμίας before using this as a direct-award finding.**

### Data entry / misclassification errors found (PDF-verified)

- **ΡΩΖΡΩΛΚ-Ω5Σ (€482,360 → €4,824):** Diavgeia structured `awardAmount` = €482,360. PDF text and matching ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ trail (€2,998 + €1,825) confirm actual award = €4,824 gross. Typographic error in original Diavgeia submission. Not an above-threshold finding; removed from findings table.
- **912ΡΩΛΚ-ΨΧΠ (€91,460 BRINX — competitive tender, not direct award):** PDF is the minutes of the Οικονομική Επιτροπή ratifying the 5th stage of an open electronic tender (ΑΠΟΦ. 458/2020). Four companies bid. BRINX won Lots C-D competitively; ΟΙΚΟΝΟΜΟΤΕΧΝΙΚΗ ΑΕΒΕ won Lot B. Total study budget €131,774. The Δ.1 classification in Diavgeia is a misclassification of a competitive award ratification — the same pattern as the hospital's 739 procedural entries. Removed from findings.
- **6ΗΞ4ΩΛΚ-20Ρ (€130.5M spurious):** Lawyer's AFM 130561769 entered in amount field. Excluded from all totals.

### Pattern notes for journalist follow-up

- ΚΟΥΤΚΙΑΣ rotation pattern: individual (112437726) and company (ΚΟΥΤΚΙΑΣ & ΣΙΑ ΕΕ, 801089930) alternating in consecutive contract years for identical building security service. August 2022 award had no supplier AFM in structured Diavgeia data — confirmed via PDF as ΚΟΥΤΚΙΑΣ ΠΑΝΑΓΙΩΤΗΣ ΙΔΙΩΤΙΚΗ ΕΠΙΧΕΙΡΗΣΗ ΠΑΡΟΧΗΣ ΥΠΗΡΕΣΙΩΝ ΑΣΦΑΛΕΙΑΣ, AFM 112437726. Same individual entity also appears at ΔΕΥΑ Λαμίας and Δήμος Στυλίδας for small amounts, confirming real operating contractor — the issue is procedural (no competitive tender), not phantom entity.
- Total ΚΟΥΤΚΙΑΣ security spend at Δήμος Λαμίας 2022–2023: €222,397 across three decisions, zero competitive tenders.

---

*Data sources: Diavgeia OpenData API (diavgeia.gov.gr) · Period: January 2020 – May 2026*
*Processing pipeline: windowed search-export collection + targeted detail hydration (types Δ.1, Δ.2.2, Γ.3.4)*
*Data quality audit: May 2026 — classification of ΑΝΑΘΕΣΗ subjects, supplier coverage analysis*
*Code and data: available in this repository under `scripts/` and `data/normalized/`*
