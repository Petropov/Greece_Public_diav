# Intelligence Finding: ORG 50200 — €4.96M Pine Cone Direct Award

**Classification:** High Priority — Procurement Irregularity  
**Organisation:** ΑΠΟΚΕΝΤΡΩΜΕΝΗ ΔΙΟΙΚΗΣΗ ΘΕΣΣΑΛΙΑΣ-ΣΤΕΡΕΑΣ ΕΛΛΑΔΑΣ (ORG 50200)  
**Decision type:** Δ.1 (Direct Award — ΑΝΑΘΕΣΗ ΕΡΓΩΝ / ΠΡΟΜΗΘΕΙΩΝ / ΥΠΗΡΕΣΙΩΝ / ΜΕΛΕΤΩΝ)  
**ADA:** `6696ΟΡ10-ΚΙΗ`  
**Date:** 14 May 2021  
**Amount:** **€4,960,000**  
**Supplier:** ΜΑΝΑΡΙΤΣΑΣ ΕΥΑΓΓΕΛΟΣ-ΚΩΝΣΤΑΝΤΙΝΟΣ (AFM 054537472)

---

## Summary

A single natural person received a direct award of **€4,960,000** from the Decentralised Administration of Thessaly-Central Greece (the regional oversight body responsible for, among other things, forestry management) for *pine cone collection from two specific named forest stands* in Northern Evia. This amount is **165× the legal ceiling** for direct awards under Art. 118 of Law 4412/2016 (capped at €30,000). No competitive tender was held.

---

## Red Flags (7 independent indicators)

### RF-1: Amount far exceeds the legal cap (×165)
Art. 118 of Law 4412/2016 caps direct awards without tender at **€30,000** (net of VAT). The awarded amount of €4,960,000 is 165 times this limit. There is no legal basis for awarding a €4.96M contract as a Δ.1 direct award.

### RF-2: Wrong CPV code — medical supplies for a forestry contract
The €4.96M contract is coded with CPV **33141600-6** ("Συλλέκτες και συσσωρευτές χωρίς βελόνες" — medical collectors/accumulators). The correct CPV for pine cone collection is **77230000-1** (Forestry services) — which was correctly used by the same supplier on a smaller €4,000 contract two days earlier (ADA: `9Τ69ΟΡ10-Ω75`). The wrong CPV classification would effectively hide this contract from forestry-category searches and audits.

### RF-3: Recipient is a natural person not found in ΓΕΜΗ
ΜΑΝΑΡΙΤΣΑΣ ΕΥΑΓΓΕΛΟΣ-ΚΩΝΣΤΑΝΤΙΝΟΣ (AFM 054537472) is a natural person with no company registration found in the ΓΕΜΗ business registry. A contract of €4.96M should have gone to a registered legal entity with the technical and financial capacity to execute a multi-million euro forestry operation.

### RF-4: The smaller contract establishes the pattern — and the price discrepancy
Two days before the €4.96M contract was signed (12 May 2021), a **€4,000** direct award was made to the same person (ADA: `9Τ69ΟΡ10-Ω75`) for the same service: *pine cone collection including transport to the ΚΕΕΠΔΑΠΥ processing center in Aμυγδαλέζα*. At an implicit unit rate of ~€4,000/job, the €4.96M contract would represent approximately 1,240 comparable collection jobs — an implausibly large scale for a single direct award issued on the same day.

### RF-5: State paying to harvest private forest owners' produce
The contract specifies collection from:
- **"ΙΔΙΩΤΙΚΟ ΔΑΣΟΣ ΠΡΟΚΟΠΙΟΥ-ΔΡΑΖΙΟΥ"** (Private forest of Prokopion-Drazion) — Συστάδα 15α
- **"ΔΙΑΚΑΤΕΧΟΜΕΝΟ ΔΑΣΟΣ ΚΕΧΡΙΩΝ-ΛΟΓΓΟΥ"** (Communally-held forest of Kechria-Longou) — Συστάδα 2β

For a private forest, the forest owner (not the state) should be the economic beneficiary of any pine cone harvest. The Decentralised Administration's role is supervisory (it manages forest districts on behalf of the state); paying nearly €5M to collect produce from a private forest stand is structurally anomalous.

### RF-6: No payment decisions in the data
No corresponding payment authorisations (ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ, Β.2.2) or commitment decisions (ΑΝΑΛΗΨΗ ΥΠΟΧΡΕΩΣΗΣ, Β.1.3) to AFM 054537472 are present in the normalised dataset. This could indicate either: (a) data not yet extracted from raw JSONs, or (b) the payment was made through a different channel, or (c) the contract was not executed.

### RF-7: Supplier's contract history shows versatility incompatible with one-person operation
The same AFM (054537472 — a natural person) signed:
- 2020-05-07: ΣΥΜΒΑΣΗ for "ΤΕΧΝΙΚΗ ΔΙΕΥΘΕΤΗΣΗ ΧΕΙΜΑΡΡΟΥ" (channel regulation construction, Γ.3.4 personal contract, €51k)
- 2020-11-13: Αναθεση for "Συντήρηση υδατοδεξαμενών αντιπυρικής προστασίας" (fire protection water tank maintenance, €22k)
- 2021-05-12: Pine cone collection service €4,000
- 2021-05-14: Pine cone collection from two large stands, €4,960,000

A single natural person performing hydraulic construction, fire infrastructure maintenance, forestry collection, and now a €4.96M collection operation is unusual without a company/team structure.

---

## Comparison with Peer Contracts

| Contract | Date | Amount | Supplier | Tender? |
|----------|------|--------|----------|---------|
| Small pine cone collection | 2021-05-12 | €4,000 | ΜΑΝΑΡΙΤΣΑΣ | Direct (Art. 118) |
| **Large pine cone collection** | **2021-05-14** | **€4,960,000** | **ΜΑΝΑΡΙΤΣΑΣ** | **Direct (Art. 118 — void)** |
| Heating oil 2026-2027 | 2026-03-03 | €679,272 | ΝΙΚΟΠΟΥΛΟΣ ΟΕ (082453034) | Direct — justification unclear |
| Fuel supply 2022 (άγονα τμήματα) | 2022-01-10 | €348,792 | — | Direct — failed-tender parts |
| Cleaning services | 2024-12-18 | €200,007 | — | **Direct (year-end, T6)** |
| Drone purchase | 2020-11-25 | €147,560 | — | **Direct (Art. 118 — ×4.9 over cap)** |
| Similar forestry services (CPV 77) in ORG 50200 | Various | €1k–€49k | Multiple | Direct |
| Flood restoration construction | 2021 | €5,200,000 | ΧΑΤΖΗΓΑΚΗΣ ΤΕΧ. ΑΕ | **Open tender** ✓ |

The €5.2M flood restoration project with ΧΑΤΖΗΓΑΚΗΣ was properly tendered. The €4.96M pine cone collection, the €679k heating oil, and the €200k cleaning award were not.

---

## Recommended Actions

1. **ΓΕΜΗ search**: Verify whether AFM 054537472 is a natural person or unregistered business. Check whether any company has ΜΑΝΑΡΙΤΣΑΣ as director or beneficial owner.

2. **Verify contract execution**: Request ΚΕΕΠΔΑΠΥ (Αμυγδαλέζα) records for deliveries of Haleppo pine cones from Συστάδα 15α / Συστάδα 2β in 2021. If no delivery record exists, the contract was not performed.

3. **Legality review chain**: Check whether ΑΔΕΔΔ (the Decentralised Administration's internal audit) reviewed this contract as legally required. Decision Ω0ΟΟΟΡ10-Ζ4Τ (legality review, July 2021) covers a different €5.2M contract — no legality review for `6696ΟΡ10-ΚΙΗ` was found in the data.

4. **CPV mismatch**: The use of medical CPV 33141600-6 for a forestry contract warrants investigation as potential deliberate misclassification.

5. **Referral**: This finding should be referred to:
   - **Γενικός Επιθεωρητής Δημόσιας Διοίκησης (ΓΕΔΔ)**
   - **Ελεγκτικό Συνέδριο** — expenditure audit
   - **Αρχή Εξέτασης Προδικαστικών Προσφυγών (ΑΕΠΠ)** — procurement law violation
   - **OLAF** if EU funds were involved in the wider Decentralised Administration budget

---

## Data Source

- Diavgeia API: `https://diavgeia.gov.gr/luminapi/api/decisions/6696ΟΡ10-ΚΙΗ`
- Comparison ADA: `9Τ69ΟΡ10-Ω75` (€4k same supplier, correct CPV)
- Normalised CSV: `data/normalized/org=50200/decisions.csv`
- Detected by: T1 (62% spend share), T8 (78% 2021 annual share), T5 (emergency classification)

*All data from open government sources. This is an analytical finding warranting scrutiny — not a legal determination.*
