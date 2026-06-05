# Intelligence Tenets — Greece Public Procurement Analysis

> These are the analytical north stars for this project. Each tenet is a
> provable, data-driven signal that can surface procurement irregularities
> using only open government data (Diavgeia, ΓΕΜΗ, ΚΗΜΔΗΣ, TED).

---

## T1 · Direct Award Concentration
**What:** Contracts issued without competitive tender (άμεση ανάθεση).  
**Signal:** Supplier receiving >25% of spend or >30% of count in org's total direct awards (DIRECT_AWARD_TYPES only) across all years with data.  
**Why it matters:** Greek law (4412/2016 Art. 118) caps direct awards at €30k. Repeated use to the same supplier is a proxy for a captured procurement process.  
**Data:** `decision_type = ΑΝΑΘΕΣΗ`, `supplier_tax_id`, `amount`, `issue_date`  
**Status:** ✅ Implemented (`detect_signals.py T1`)  
**Fires in:** 3/23 orgs — ORG 50200 (ΜΑΝΑΡΙΤΣΑΣ 62%), ΝΟΣ. ΛΑΜΙΑΣ (ISS/UNISON 27%), ΝΟΣ. ΤΡΙΚΑΛΩΝ (ΓΕΝ ΚΑ ΑΕ 34%)  
**Caveats:** Limited by sparse supplier_tax_id coverage (~2–10% of decisions). Includes AFM-as-amount filter and self-referential org-VAT exclusion.

---

## T2 · Threshold Structuring (Contract Splitting)
**What:** A single economic need split into multiple contracts, each just below a legal threshold (€30k direct award, €140k TED), to avoid competitive tendering.  
**Signal:** Same supplier, same org, same CPV category, multiple awards within 90 days, individual amounts <threshold but sum >threshold.  
**Why it matters:** Explicitly prohibited by Art. 6 of Dir. 2014/24/EU and Art. 6 of Law 4412/2016. EPPO jurisdiction above €10k EU-funded contracts.  
**Data:** `supplier_tax_id`, `amount`, `issue_date`, `cpv`, `subject`  
**Status:** ✅ Implemented (`patch_amounts.py` + structuring report)

---

## T3 · Cross-Organisation Benchmark
**What:** Compare equivalent organisations (hospitals, municipalities) on key procurement ratios: % direct awards, cost per unit (bed, resident, student), supplier concentration index.  
**Signal:** An org that is 2σ above peer mean on direct award rate or cost-per-unit is an outlier worth investigating.  
**Why it matters:** Absolute numbers are hard to contextualise; relative deviation against peers is actionable.  
**Data:** Multi-org pipeline output, normalised by org size metric  
**Status:** 🔄 In progress (hospital benchmark running)

---

## T4 · Contract Amendment Inflation
**What:** A contract signed below a tender threshold is later amended upward — sometimes multiple times — until the real value far exceeds what would have required competitive bidding.  
**Signal:** Sum of (original award + all amendments) / original award > 1.5×, or amended value crosses a legal threshold.  
**Why it matters:** Amendments are the most common mechanism to avoid re-tendering on large contracts. Construction and IT are highest-risk categories.  
**Data:** `relatedDecisions` links in hydrated JSONs, lifecycle deduplication  
**Status:** ⏳ Pending — lifecycle linking partially implemented, amendment detection not yet built

---

## T5 · Emergency Procurement Overuse
**What:** Procurement via "negotiated procedure without prior publication" (διαδικασία διαπραγμάτευσης χωρίς προηγούμενη δημοσίευση) — the emergency bypass of competitive tendering.  
**Signal:** Org where emergency procurement accounts for >20% of total spend, or sustained use outside a declared emergency period.  
**Why it matters:** Art. 32 of Dir. 2014/24/EU and Art. 32 of Law 4412/2016 restrict emergency use to genuine urgency. COVID-era abuse was systematic and well-documented across Europe.  
**Data:** `decision_type`, `subject` keywords (διαπραγμάτευση, κατεπείγον)  
**Status:** ⏳ Pending — data available, detector not yet built

---

## T6 · Temporal Clustering
**What:** Unusual concentration of awards at specific times: last 10 days of December (budget absorption), pre-election windows (3–6 months before municipal/regional elections), or weekends and public holidays (low-scrutiny windows).  
**Signal:** % of annual spend issued in December last-10-days > 30%; award rate in pre-election quarter >2× baseline.  
**Why it matters:** Year-end budget dumps and pre-election patronage are well-established patterns in Greek local government. Temporal clustering is the quantitative fingerprint.  
**Data:** `issue_date`, election calendar  
**Status:** ⏳ Pending — data available, detector not yet built

---

## T7 · Signer–Supplier Network
**What:** The official who signs the procurement decision (or their immediate family / business associate) appears as director or beneficial owner of the winning supplier.  
**Signal:** Match between `signerIds` in Diavgeia and `persons` (directors/owners) in ΓΕΜΗ across the suppliers receiving awards from that signer's org.  
**Why it matters:** This is the highest-evidentiary-value corruption signal — it places a named individual at the intersection of the awarding authority and the beneficiary.  
**Data:** Diavgeia `signerIds` → ΓΕΜΗ `persons` cross-reference  
**Status:** ⏳ Pending — requires ΓΕΜΗ persons enrichment per supplier + signer ID resolution

---

## T8 · Single-Source Monopoly
**What:** One supplier captures >60% of a specific CPV category at an org for 3+ consecutive years, exclusively through direct awards.  
**Signal:** `supplier_tax_id` share of `cpv_prefix` spend per year, sustained without open tender.  
**Why it matters:** Long-term exclusive relationships without competition are the structural equivalent of a captured market. The INTERKAT / Lamia Hospital H/M case is the prototype.  
**Data:** `cpv`, `supplier_tax_id`, `amount`, `issue_date`, `decision_type`  
**Status:** ✅ Detected manually (INTERKAT); automated detector pending

---

## T9 · TED Gap — EU Threshold Evasion
**What:** Contracts above the EU publication threshold (€140k works, €144k supplies/services for central government, lower for utilities) that appear in Diavgeia but not in TED (Tenders Electronic Daily).  
**Signal:** Diavgeia `amount` ≥ €140k + `decision_type = ΑΝΑΘΕΣΗ` + no matching record in TED for the same supplier/org/period.  
**Why it matters:** Publication in TED is a legal obligation under Dir. 2014/24/EU. Absence is a possible Art. 4 violation and triggers EPPO jurisdiction for EU-funded contracts.  
**Data:** Diavgeia amounts + TED API (`ted.europa.eu/api`)  
**Status:** ⏳ Pending — TED API cross-reference not yet implemented

---

## T10 · ΚΗΜΔΗΣ Orphan Payments
**What:** Payment decisions (ΟΡΙΣΤΙΚΟΠΟΙΗΣΗ ΠΛΗΡΩΜΗΣ) that reference ADAM contract numbers which do not exist in the ΚΗΜΔΗΣ contract registry.  
**Signal:** `adam_number` in Diavgeia payment decision → lookup in ΚΗΜΔΗΣ returns 404 or no match.  
**Why it matters:** A payment with no traceable registered contract is a red flag for off-books disbursement. ΚΗΜΔΗΣ registration is mandatory for all public contracts above €1k.  
**Data:** Diavgeia `relatedDecisions` / `adamNumber` fields + ΚΗΜΔΗΣ API  
**Status:** ⏳ Pending — ADAM field extraction not yet implemented

---

## T11 · ΓΕΜΗ Red Flags on Winning Suppliers
**What:** Structural anomalies in the company registry of suppliers receiving public contracts.  
**Signals (tiered):**
- Company registered *after* the contract award date
- Registered capital below contract value (undercapitalised)
- No activity code (KAD) matching the contract subject
- Company marked inactive / struck off while still receiving payments
- Company registered <6 months before first award  
**Why it matters:** Shell companies and paper entities are the financial vehicle of choice for procurement fraud. ΓΕΜΗ provides the paper trail.  
**Data:** ΓΕΜΗ enrichment (already implemented), cross-referenced with `supplier_tax_id` and `issue_date`  
**Status:** ✅ Implemented (enrich_gemi.py); red flag scoring not yet consolidated

---

## T12 · Price Benchmark Across Organisations
**What:** Same CPV code, same item category, dramatically different unit prices across comparable organisations in the same period.  
**Signal:** Price for CPV `X` at org A is >2× median across peer orgs in the same year.  
**Why it matters:** Price outliers indicate either incompetence, fraudulent invoicing, or collusion with a favoured supplier. Hospital procurement of consumables is the highest-signal category.  
**Data:** Multi-org `amount` + `cpv` + normalisation denominator (beds, population)  
**Status:** ⏳ Pending — requires hospital benchmark data (pipeline running)

---

## Implementation Roadmap

| # | Tenet | Effort | Data ready? | Priority |
|---|-------|--------|-------------|----------|
| T1 | Direct award concentration | Low | ✅ | Done ✅ |
| T2 | Structuring | Low | ✅ | Done |
| T3 | Cross-org benchmark | Medium | 🔄 | High |
| T8 | Single-source monopoly | Low | ✅ | High |
| T5 | Emergency procurement overuse | Low | ✅ | High |
| T6 | Temporal clustering | Low | ✅ | High |
| T11 | ΓΕΜΗ red flags | Low | ✅ | High |
| T4 | Amendment inflation | Medium | Partial | Medium |
| T12 | Price benchmark | Medium | 🔄 | Medium |
| T9 | TED gap | Medium | ❌ | Medium |
| T10 | ΚΗΜΔΗΣ orphans | Medium | ❌ | Low |
| T7 | Signer–supplier network | High | Partial | Low |

---

*All signals are derived exclusively from open government data. Nothing here constitutes a legal finding — these are patterns that warrant scrutiny by competent authorities.*
