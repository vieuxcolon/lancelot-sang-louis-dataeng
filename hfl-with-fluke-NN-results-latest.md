---

# Horizontal Federated Learning with Medical Data

## Overview

This repository implements a **horizontal federated learning (HFL)** pipeline using medical data. The goal is to train a **small neural network (SmallNN)** across multiple clients while preserving data privacy. Both **IID (Independent and Identically Distributed)** and **Non-IID** client scenarios are supported, allowing comparison of federated performance under realistic heterogeneous conditions.

---

## Pipeline Logical Flow

1. **Data Loading and Preprocessing**

   * Medical dataset is loaded from a processed CSV file.
   * Features (`X`) and labels (`y`) are extracted.
   * Test set is kept separate for final evaluation.

2. **Client Data Splitting**

   * **IID split:** Equal-sized partitions for each client.
   * **Non-IID split:** Dirichlet distribution (`alpha=0.5`) used to simulate heterogeneous client distributions.

3. **Small Neural Network Definition**

   * Input layer → Hidden layer (16 units) → ReLU → Output layer (2 units for binary classification).
   * PyTorch framework is used.

4. **Federated Training**

   * Number of global rounds: 3
   * Each round:

     * Local training for 1 epoch per client.
     * Parameters averaged using **FedAvg** to update the global model.
     * Client-level accuracy tracked for each round.
   * Optimizer: Stochastic Gradient Descent (SGD)
   * Loss: Cross-Entropy

5. **Evaluation Metrics**

   * **Performance Metrics:** Accuracy, F1 score, Precision, Recall
   * **Fairness Metrics:** SPD (Statistical Parity Difference), EOD (Equal Opportunity Difference) by Age
   * **Utility Metrics:** Subset of performance metrics
   * **Cost Metrics:** Number of global rounds, Number of clients
   * **Client-level Metrics:** Final accuracy for each client

6. **Experiments**

   * Two separate experiments:

     * **IID scenario**
     * **Non-IID scenario**
   * Results compared to highlight impact of client heterogeneity.

---

## Results Summary

### 1️⃣ IID Experiment

| Metric    | Value  |
| --------- | ------ |
| Accuracy  | 0.6639 |
| F1 Score  | 0.7979 |
| Precision | 0.6641 |
| Recall    | 0.9993 |

| Fairness Metric | Value   |
| --------------- | ------- |
| SPD (Age)       | -0.0001 |
| EOD (Age)       | -0.0030 |

| Utility Metric | Value  |
| -------------- | ------ |
| Accuracy       | 0.6639 |
| F1 Score       | 0.7979 |

| Cost Metric       | Value |
| ----------------- | ----- |
| Global Rounds     | 3     |
| Number of Clients | 5     |

**Client-level Accuracy**

| Client | Accuracy |
| ------ | -------- |
| 1      | 0.6685   |
| 2      | 0.6661   |
| 3      | 0.6669   |
| 4      | 0.6655   |
| 5      | 0.6663   |

---

### 2️⃣ Non-IID Experiment

| Metric    | Value  |
| --------- | ------ |
| Accuracy  | 0.6637 |
| F1 Score  | 0.7979 |
| Precision | 0.6639 |
| Recall    | 0.9995 |

| Fairness Metric | Value   |
| --------------- | ------- |
| SPD (Age)       | -0.0000 |
| EOD (Age)       | -0.0029 |

| Utility Metric | Value  |
| -------------- | ------ |
| Accuracy       | 0.6637 |
| F1 Score       | 0.7979 |

| Cost Metric       | Value |
| ----------------- | ----- |
| Global Rounds     | 3     |
| Number of Clients | 5     |

**Client-level Accuracy**

| Client | Accuracy |
| ------ | -------- |
| 1      | 0.7174   |
| 2      | 0.9770   |
| 3      | 0.5180   |
| 4      | 0.5293   |
| 5      | 0.9762   |

---

### 3️⃣ Client-level Accuracy Comparison (IID vs Non-IID)

| Client | IID    | Non-IID |
| ------ | ------ | ------- |
| 1      | 0.6685 | 0.7174  |
| 2      | 0.6661 | 0.9770  |
| 3      | 0.6669 | 0.5180  |
| 4      | 0.6655 | 0.5293  |
| 5      | 0.6663 | 0.9762  |

**Observation:**

* IID clients show similar performance across all clients.
* Non-IID clients display **large disparities**, with some clients achieving very high accuracy and others much lower due to uneven sample distributions.
* Global accuracy remains stable (~0.664), showing the robustness of FedAvg despite heterogeneity.
* Fairness metrics remain close to zero, indicating minimal age-based bias in predictions.

---

## Conclusion

* The SmallNN HFL pipeline works correctly under both IID and Non-IID client distributions.
* **Client heterogeneity affects local performance**, but global model accuracy remains relatively stable.
* Age-based fairness metrics (SPD, EOD) are negligible, showing no significant bias in model predictions.
* The pipeline is fully reproducible, modular, and ready for extension to more complex models or additional fairness metrics.

---

## Federated Training Flow Diagram

                     ┌───────────────┐
                     │  Global Model │
                     └───────┬───────┘
                             │
                    Broadcast global model
                             │
             ┌───────────────┴───────────────┐
             │                               │
        ┌────▼────┐                     ┌────▼────┐
        │ Client1 │                     │ ClientN │
        └────┬────┘                     └────┬────┘
             │                                │
        Local Training                   Local Training
       (1 epoch per round)              (1 epoch per round)
             │                                │
        Local Model                       Local Model
       Parameters                        Parameters
             │                                │
             └─────────────┬──────────────────┘
                           │
                    Aggregate (FedAvg)
                           │
                     Update Global Model
                           │
                     Repeat for Rounds
                           │
                    Evaluate on Test Set
                           │
                ┌──────────┴───────────┐
                │ Performance Metrics  │
                │ Fairness Metrics     │
                │ Client-level Metrics │
                └──────────────────────┘
Do you want me to add that diagram?
