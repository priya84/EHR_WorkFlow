# EHR Workflow: Scalable Spark ML Pipeline for Large-Scale Electronic Health Records

> **A cloud-based, distributed PySpark/Spark ML workflow for transforming large-scale, heterogeneous electronic health record (EHR) data into machine-learning-ready features, with an application to post-traumatic epilepsy (PTE) risk prediction among patients with traumatic brain injury (TBI).**

## 🔬 Project Overview

This repository contains the custom PySpark/Spark ML implementation supporting the manuscript:

**“Scalable Feature Engineering and Machine Learning on Large-Scale Electronic Health Records in the Cloud: A Spark ML Workflow for Assessing Risk for Post-Traumatic Epilepsy.”**

The framework is designed to address a central challenge in EHR-based machine learning: clinical data are typically large, heterogeneous, sparse, high-dimensional, and difficult to process efficiently with conventional single-node workflows. The implementation combines cohort construction, demographic processing, diagnosis/comorbidity normalization, medication standardization, laboratory preprocessing, scalable feature integration, class-balance processing, comparative machine learning, ensemble modeling, and statistical evaluation.

The workflow was developed and validated using the **Oracle Real-World Data (ORWD)** warehouse (formerly Cerner Real-World Data) and is designed to be adaptable to other appropriately provisioned EHR environments.

In the associated study, the workflow reduced approximately **90,000 raw variables to 11,265 normalized predictors**. The reported stacked-tree ensemble achieved **AUC = 0.916, sensitivity = 84.6%, and specificity = 82.4%** for PTE risk classification.

### Key Features

- ⚡ **Distributed EHR processing:** PySpark-based processing for large, high-dimensional clinical datasets
- 🧹 **Cohort and control construction:** Rule-based filtering, temporal checks, TBI/PTE identification, control preparation, and matching
- 🩺 **Comorbidity normalization:** ICD-9/ICD-10 identification, ICD-9 → ICD-10 mapping, code cleaning, truncation, and diagnosis grouping
- 💊 **Medication normalization:** RxNorm-based medication standardization, variant consolidation, and pharmacologic grouping
- 🧪 **Laboratory processing:** Sparse-feature filtering, interpretation normalization, mode-based selection, and binary transformation
- 🧱 **Memory-efficient feature engineering:** Cycle-based row/column partitioning, controlled pivoting, caching/checkpoint-oriented processing, broadcast joins, and repartitioned joins
- ⚖️ **Class-balance processing:** Stratified sampling, resampling, and minority-class oversampling workflows
- 🤖 **Comparative Spark ML modeling:** Logistic Regression, Naïve Bayes, SVM, Decision Tree, Random Forest, Gradient-Boosted Trees, Multi-Layer Perceptron, and regularized variants
- 🧠 **Stacked ensemble learning:** Decision Tree, Random Forest, and Gradient-Boosted Tree base learners combined using a Logistic Regression meta-classifier
- 📊 **Evaluation and statistical validation:** AUC-ROC, sensitivity/recall, specificity, threshold-based classification, three-fold cross-validation, hyperparameter tuning, and pairwise statistical comparison
- ☁️ **Cloud-oriented design:** Distributed execution optimized for wide EHR feature spaces and resource-constrained cloud environments

## 🚀 Workflow Overview

The implementation follows the major stages below:

1. **Cohort and control construction**
2. **Demographic feature preparation**
3. **Diagnosis and comorbidity extraction**
4. **ICD normalization and feature aggregation**
5. **Medication extraction and RxNorm normalization**
6. **Laboratory cleaning and normalization**
7. **Scalable feature integration using Spark joins and pivot operations**
8. **Stratified sampling and class-balance processing**
9. **Spark ML preprocessing and model training**
10. **Stacked ensemble construction**
11. **Cross-validation, threshold evaluation, and statistical comparison**

## 🏗️ Workflow Architecture

The pipeline follows four sequential stages, moving from raw EHR extraction through normalization to model training:

```
┌─────────────────────┐     ┌──────────────────────┐     ┌───────────────────────┐     ┌────────────────────┐
│ 1. Cohort & Control  │ --> │ 2. Cohort-Control     │ --> │ 3. Feature             │ --> │ 4. Normalization &  │
│    Extraction        │     │    Matching & Stackup │     │    Normalization       │     │    ML Pipeline      │
│  (ICD-9/10 case      │     │  (demographic/temporal│     │  (comorbidity, Rx-Norm │     │  (stratified sampling,│
│   identification,    │     │   matching, superset   │     │   medications, lab      │     │   stacking ensemble, │
│   AED verification)  │     │   assembly)            │     │   binarization)         │     │   evaluation)        │
└─────────────────────┘     └──────────────────────┘     └───────────────────────┘     └────────────────────┘
```


## 🏗️ Processing Architecture

### 1. Cohort and Control Extraction

The workflow identifies TBI and PTE cohorts using ICD-9-CM/ICD-10-CM diagnosis information and anti-epileptic drug (AED) prescription information. The source implementation also applies exclusion criteria for pre-existing epilepsy/seizure history and other conditions associated with independent seizure risk.

### 2. Cohort-Control Matching

A TBI control cohort without epilepsy is constructed using demographic and temporal matching variables, including age group, sex, race, and medical-history information, to reduce confounding and sampling bias.

### 3. Comorbidity Processing

```text
Raw diagnosis/comorbidity records
        ↓
ICD-9 / ICD-10 identification
        ↓
ICD-9 → ICD-10 mapping
        ↓
Code cleaning and truncation
        ↓
Higher-level diagnosis grouping
        ↓
ML-ready comorbidity features
```

The workflow converts ICD-9 codes to ICD-10 where required and groups cleaned diagnosis codes into higher-level categories to reduce dimensionality while retaining clinically meaningful information.

### 4. Medication Processing

```text
Raw medication records
        ↓
Text preprocessing
        ↓
RxNorm-based normalization
        ↓
Duplicate / variant consolidation
        ↓
Pharmacologic-class grouping
        ↓
ML-ready medication features
```

The source workflow standardizes raw medication strings through RxNorm-based processing and consolidates medication variants into normalized entries and pharmacologic classes.

### 5. Laboratory Processing

```text
Raw laboratory records
        ↓
Sparse-code filtering
        ↓
Interpretation normalization
        ↓
Mode-based selection
        ↓
Binary feature transformation
        ↓
ML-ready laboratory features
```

Longitudinal laboratory values are cleaned, sparse variables are filtered, interpretation categories are normalized, and categorical outcomes such as low/normal/high/abnormal/positive/negative are converted into machine-learning-ready representations.

### 6. Scalable Spark Transformation

```text
Demographics ───────┐
Comorbidities ──────┤
Medications ────────┼──► Partition / Pivot / Normalize
Laboratory features ┘              ↓
                          Optimized Spark joins
                                   ↓
                          Integrated feature table
                                   ↓
                             Spark ML pipeline
```

## ⚙️ Scalability Strategy

The workflow uses several techniques to make high-cardinality EHR feature engineering practical in distributed environments:

- **Cycle-based partitioning** for high-cardinality transformations
- **Row-wise and column-wise splitting** before expensive pivot operations
- **Controlled pivoting** with `pivotMaxValues = 10000`
- **Broadcast joins** for smaller tables such as demographics
- **Repartitioning by patient identifier** for large-table joins to reduce shuffle overhead
- **Caching and checkpoint-oriented processing** for long Spark workflows
- **Modular notebook stages** to improve reproducibility and fault tolerance
- **Distributed execution on CPU-based cloud infrastructure** without requiring GPU hardware for the feature-engineering pipeline

## 🤖 Machine Learning Models

The repository includes workflows for evaluating the following Spark ML classifiers:

- Logistic Regression
- Logistic Regression with regularization / elastic net
- Naïve Bayes
- Support Vector Machine
- Support Vector Machine with regularization
- Decision Tree
- Decision Tree with regularization
- Random Forest
- Random Forest with regularization
- Gradient-Boosted Trees
- Gradient-Boosted Trees with regularization
- Multi-Layer Perceptron
- Stacked-tree ensemble with a Logistic Regression meta-classifier

### Stacked Ensemble

```text
Decision Tree ───────┐
Random Forest ───────┼──► Class-1 probabilities ──► Logistic Regression ──► Final risk score
Gradient Boosted Tree┘
```

The ensemble combines predictions from complementary tree-based base learners and uses Logistic Regression as the meta-classifier.

## 📈 Evaluation and Reported Performance

Model evaluation includes:

- **Three-fold cross-validation**
- **Sensitivity / Recall**
- **Specificity**
- **AUC-ROC**
- **Threshold-based classification**
- **Regularization and hyperparameter tuning**
- **Pairwise statistical comparison of model performance**
- **Paired t-tests across model comparisons**

Selected results reported in the source README are summarized below:

| Model | AUC | Sensitivity | Specificity |
|---|---:|---:|---:|
| **Stacked-Tree Ensemble (LR meta-classifier)** | **0.916** | **84.6%** | **82.4%** |
| Gradient-Boosted Trees (regularized) | 0.92 | 79% | 90% |
| Logistic Regression (elastic net) | — | 76% | — |
| SVM (L2 regularized) | — | 73% | — |

For the stacked ensemble at a classification threshold of `0.1`, the source README reports **TP = 1,295, TN = 8,457, FP = 1,805, and FN = 235**. Full comparative results and statistical analyses should be interpreted with the associated manuscript.

## ☁️ Computational Environment

The manuscript-aligned implementation was designed for an AWS/YARN-based distributed Spark environment.

- **Platform:** AWS with YARN-based Spark execution
- **Cluster:** 8 nodes
- **Per-node hardware:** Intel Xeon Platinum 8259CL, 16 vCPUs @ 2.50 GHz, 124 GB RAM
- **Driver memory:** 16 GB
- **Spark pivot setting:** `pivotMaxValues = 10000`
- **Adaptive query execution:** enabled in the source configuration
- **Reported scale:** 1M+ patient records processed during full-scale normalization and feature integration
- **Reported runtime:** under two hours for the full-scale transformation workflow

Environment-specific paths, Spark settings, storage locations, and resource allocations may need to be updated before reuse.

## 🚀 Quick Start

### Requirements
```
pyspark
pandas
numpy
scikit-learn
requests          # for RxNorm API calls
```

## 📁 Repository Structure

The repository is notebook-oriented and retains multiple versions from iterative workflow development. The existing source structure is:
```
EHR_WorkFlow/
├── README.md
├── stage1_cohort_extraction/       # TBI/PTE cohort identification, ICD code filtering, AED verification
├── stage2_cohort_control_match/    # Matched control construction, superset assembly
├── stage3_normalization/           # Comorbidity (ICD-9→10), medication (RxNorm), and lab normalization
├── stage4_ml_pipeline/             # Stratified sampling, feature integration, model training & evaluation
├── utils/                          # Shared ETL and Spark utility functions (etl.py, util.py)
└── cluster/                        # Cluster submission and terminal execution scripts
```
```text
EHR_WorkFlow/
├── IPYNBLatestAndUpdated/
│   ├── etl.py
│   ├── util.py
│   ├── SuperSet-Cohort+ControlVersion17.ipynb
│   ├── SuperSet-Cohort+ControlVersion17-TestingwithSmallset.ipynb
│   └── Updated/
│
├── IPYNBPart1/
│   ├── SuperSet.ipynb
│   ├── SuperSet-Cohort+Control.ipynb
│   ├── SuperSet-Cohort+Control+MatchedCols-Final.ipynb
│   └── SuperSet-Cohort+ControlVersion*.ipynb
│
├── IPYNBPart2/
│   ├── TBI_EPI_Extract.ipynb
│   ├── TBI_Cohort_Prep.ipynb
│   ├── Cohort-control-pairing.ipynb
│   ├── Diagnosis.ipynb
│   ├── Descriptive Statistics.ipynb
│   └── DataPreparationModels.ipynb
│
├── IPYNBPart3/
│   ├── Epilepsy_Cohort_Creation.ipynb
│   ├── Extract_Commo_Cohort_Control.ipynb
│   ├── Normalization*.ipynb
│   ├── ManageBroadCastAndRepartition.ipynb
│   ├── MLPipelinePreprocess.ipynb
│   └── MLPipelineFinal.ipynb
│
├── IPYNBPart4/
│   ├── Normalized_Commorbidity_Codes_Final.ipynb
│   ├── Normalization_Medication_API_ClassName.ipynb
│   ├── NormalizationLab_Finalized.ipynb
│   ├── Strat_balance_sampling.ipynb
│   ├── Predictions.ipynb
│   └── RearrangeColumns.ipynb
│
│
└── TerminalFiles/
    ├── terminalcommands1.ipynb
    └── terminalcommands2.ipynb
```

> **Manuscript-aligned use:** Because multiple notebook versions are retained, prefer the latest/finalized notebooks where available and preserve the execution order of dependent intermediate outputs.

## 🧩 Main Utility Modules

### `etl.py`

Provides reusable extraction and transformation utilities for Spark-based clinical workflows, including:

- condition/diagnosis extraction
- procedure extraction
- medication extraction
- cohort statistics
- Spark DataFrame processing

### `util.py`

Contains helper functions for:

- Spark database/table operations
- joins
- demographic transformations
- age calculations
- cohort distributions
- exploratory summaries
- visualization utilities

## 🔧 Core Technologies and Dependencies

- **Python**
- **Apache Spark / PySpark**
- **Spark ML / MLlib**
- **Jupyter Notebook / JupyterLab**
- **Pandas**
- **NumPy**
- **PyArrow**
- **scikit-learn**
- **Matplotlib / Seaborn**
- **Requests** (for RxNorm API access)
- **RxNorm API**
- **Parquet**
- **Cloud-based distributed computing**

Install versions compatible with the Spark/Python environment used for your deployment. The repository is notebook-oriented rather than a single-command application.

## ▶️ Running the Workflow

### 1. Clone the repository

```bash
git clone https://github.com/priya84/EHR_WorkFlow.git
cd EHR_WorkFlow
```

### 2. Configure the environment

Prepare a compatible Spark/Jupyter environment and configure access to the required EHR data source. Update environment-specific file paths, storage locations, Spark settings, and credentials as needed.

### 3. Execute the notebooks in workflow order

```text
1. Configure the Spark/Jupyter environment
2. Run cohort and control extraction notebooks
3. Run demographic and diagnosis preprocessing
4. Run comorbidity normalization
5. Run medication normalization
6. Run laboratory normalization
7. Integrate normalized feature groups
8. Run stratified sampling / balancing
9. Run Spark ML preprocessing
10. Train and evaluate individual models
11. Run ensemble prediction and statistical analysis
```

## 🔬 Reproducibility Notes

To support reproducible use of the workflow:

- Use consistent Spark and Python environments across pipeline stages.
- Preserve notebook execution order for dependent intermediate outputs.
- Review and update environment-specific file and storage paths before execution.
- Keep ICD and RxNorm normalization rules consistent with the methodology described in the associated manuscript.
- Use the same sampling, cross-validation, thresholding, and model-selection procedures when attempting to reproduce reported experiments.
- Record Spark configuration, random seeds where applicable, model hyperparameters, and software/package versions for each experimental run.
- Retain clear provenance for intermediate datasets and transformations without committing protected patient-level information.

## 🔒 Data Availability and Privacy

This repository provides **code only**. No patient-level EHR data, extracted patient-level feature tables, or protected intermediate data artifacts are distributed or should be committed to the repository.

The associated study used data from the **Oracle Real-World Data (ORWD)** warehouse. The source README describes these data as de-identified and subject to Oracle Health Corporation data-use requirements. Because the underlying clinical data are not publicly redistributed, researchers seeking to reproduce or adapt the workflow must obtain their own authorized access to ORWD or an equivalent EHR data source and comply with all applicable institutional, contractual, privacy, and data-use requirements.

This separation between public code and restricted clinical data is consistent with the JHIR/Springer Nature expectation that, when research data cannot be shared publicly, the availability statement should explain the access restrictions and conditions for reuse.

## 💻 Code Availability

The custom PySpark/Spark ML workflow is provided in this repository. Environment-specific infrastructure, credentials, proprietary EHR access, and restricted patient-level data are not included.
> **Note:** This repository contains only the pipeline code. No patient-level data, extracted feature tables, or intermediate data artifacts are included or should be committed — see **Data Availability** below.

## 🔒 Data Availability

This repository provides **code only**. No patient data, derived feature tables, or data extracts are distributed. All EHR data used in the associated study were obtained from the Oracle Real-World Data (ORWD) warehouse, fully de-identified in accordance with HIPAA, and access is governed by Oracle Health Corporation's data use requirements. Researchers wishing to apply this workflow to their own data should provision their own ORWD or equivalent EHR vendor access.

## 📚 Associated Manuscript and Citation

**Associated manuscript:**

> Priyadharsini Ramamurthy, Zheng Han, Zhuqi Miao, Dursun Delen, Andrew Gin, William Paiva, and Johnson P. Thomas. “Scalable Feature Engineering and Machine Learning on Large-Scale Electronic Health Records in the Cloud: A Spark ML Workflow for Assessing Risk for Post-Traumatic Epilepsy.” Manuscript prepared for the *Journal of Healthcare Informatics Research*.

Until final publication metadata (volume, issue, pages/article number, year, and DOI) are available, cite the work as a manuscript rather than presenting provisional bibliographic details as final publication information.

```bibtex
@article{ramamurthy_ehr_workflow,
  title  = {Scalable Feature Engineering and Machine Learning on Large-Scale Electronic Health Records in the Cloud: A Spark ML Workflow for Assessing Risk for Post-Traumatic Epilepsy},
  author = {Ramamurthy, Priyadharsini and Han, Zheng and Miao, Zhuqi and Delen, Dursun and Gin, Andrew and Paiva, William and Thomas, Johnson P.},
  note   = {Manuscript prepared for the Journal of Healthcare Informatics Research}
}
```

> **After publication:** replace the manuscript citation above with the final JHIR citation and DOI exactly as published.

## 💰 Funding / Acknowledgment

This work was supported by the **U.S. Department of Defense Congressionally Directed Medical Research Programs (CDMRP)** under Awards **HT9425-24-1-0480** and **HT9425-24-1-0481**.

## 👥 Authors

**Priyadharsini Ramamurthy¹, Zheng Han²*, Zhuqi Miao³, Dursun Delen*, Andrew Gin³, William Paiva, Johnson P. Thomas¹**

¹ Department of Computer Science, Oklahoma State University  
² Department of Biomedical Engineering, University of Central Oklahoma  
³ Center for Health Systems Innovation, Spears School of Business, Oklahoma State University  
⁴ Department of Management Science & Information Systems, Spears School of Business, Oklahoma State University  
⁵ Department of Management Information Systems, Faculty of Business Administration, Haliç University

*Corresponding authors: Zheng Han (zhan1@uco.edu) and Dursun Delen (dursun.delen@okstate.edu).*

## 📜 License

The source README identifies this project as being released under the **MIT License**. 

## 📌 Research Transparency and JHIR Alignment

This README is intended to support, rather than replace, the associated manuscript. Its organization emphasizes elements relevant to JHIR/Springer Nature research transparency and reproducibility, including:

- a clear relationship between the repository and associated manuscript;
- a reproducible description of the computational workflow and execution order;
- explicit description of code availability;
- explicit restrictions on patient-level data sharing and conditions for reuse;
- disclosure of funding support;
- consistent author and corresponding-author information; and
- citation guidance that avoids presenting non-final publication metadata as final bibliographic information.

Journal-required declarations such as ethics approval, consent, competing interests, materials availability, and author contributions belong in the manuscript/submission system and should remain consistent with the final JHIR submission. They are not duplicated here unless directly relevant to use of the repository.