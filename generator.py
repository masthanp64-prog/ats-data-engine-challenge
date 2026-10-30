"""
ATS End-to-End Workflow Data Generation Engine
Author: Masthan P (Candidate Profile)
Target Drive Round: Challenge 3 (72-Hour Project)

Design Pattern: Decoupled Transaction Engine with Rule-Based Anomaly Injections.
"""

import os
import random
import datetime
import hashlib
import logging
from typing import Dict, List, Tuple, Any
import pandas as pd
import numpy as np

# -------------------------------------------------------------------------
# 0. Global Logger and Parameter Configurations
# -------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger("ATS_Engine")


class ATSDatasetGenerator:
    def __init__(self, scale_factor: float = 1.0, seed: int = 20261022):
        self.scale_factor = scale_factor
        self.seed = seed
        self.base_date = datetime.datetime(2026, 1, 1)
        
        # Target volume limits scaling linearly
        self.num_reqs = int(100 * scale_factor)
        self.num_candidates = int(2000 * scale_factor)
        self.num_apps_target = int(3000 * scale_factor)
        self.num_interviews_target = int(600 * scale_factor)
        self.num_offers_target = int(150 * scale_factor)
        
        # Internal state metrics
        self.ground_truth_records: List[Dict[str, str]] = []
        self._set_deterministic_seed()

    def _set_deterministic_seed(self) -> None:
        """Enforces absolute reproducibility boundaries across execution runtimes."""
        random.seed(self.seed)
        np.random.seed(self.seed)

    @staticmethod
    def calculate_dataframe_checksum(df: pd.DataFrame) -> str:
        """Computes deterministic matrix verification tokens using SHA-256."""
        hash_array = pd.util.hash_pandas_object(df, index=False).values
        return hashlib.sha256(hash_array).hexdigest()

    # -------------------------------------------------------------------------
    # 1. Pipeline Segment: Foundational Master Schemas
    # -------------------------------------------------------------------------
    def generate_master_metadata(self) -> Tuple[pd.DataFrame, pd.DataFrame]:
        logger.info("Initializing structural Core Master Data...")
        
        depts = ["Engineering", "Product Management", "Data Science", "Design", "Operations", "Sales"]
        df_dept = pd.DataFrame({
            "dept_id": [f"DEPT-{i+1:03d}" for i in range(len(depts))],
            "dept_name": depts
        })

        skills = ["Python", "Java", "Go", "SQL", "Machine Learning", "System Design", "Product Strategy", "UI/UX", "Kubernetes", "AWS"]
        df_skill = pd.DataFrame({
            "skill_id": [f"SKILL-{i+1:03d}" for i in range(len(skills))],
            "skill_name": skills
        })
        
        return df_dept, df_skill

    def generate_job_requisitions(self, df_dept: pd.DataFrame) -> pd.DataFrame:
        logger.info(f"Generating {self.num_reqs} master Job Requisition matrices...")
        roles = ["Software Engineer", "Data Scientist", "Product Manager", "UX Designer", "DevOps Engineer"]
        dept_ids = df_dept["dept_id"].tolist()
        
        req_records = []
        for i in range(self.num_reqs):
            req_id = f"REQ-2026-{i+1:06d}"
            min_sal = random.randint(60000, 110000)
            max_sal = min_sal + random.randint(25000, 55000)
            
            # Enforce systemic baseline closure (Leaving the last 10 rows closed intentionally)
            status = "OPEN" if i < (self.num_reqs - 10) else "CLOSED"
            
            req_records.append({
                "requisition_id": req_id,
                "title": random.choice(roles),
                "dept_id": random.choice(dept_ids),
                "min_salary": float(min_sal),
                "max_salary": float(max_sal),
                "status": status
            })
        return pd.DataFrame(req_records)

    def generate_candidates(self, df_skill: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        logger.info(f"Modeling {self.num_candidates} non-sensitive synthetic Candidate profiles...")
        f_names = ["Amit", "Priya", "Rahul", "Neha", "Vijay", "Ananya", "Rajesh", "Kiran", "Suresh", "Lakshmi"]
        l_names = ["Sharma", "Verma", "Kumar", "Singh", "Patel", "Reddy", "Nair", "Joshi", "Das", "Mehta"]
        skill_ids = df_skill["skill_id"].tolist()
        
        cand_records = []
        bridge_records = []
        
        for i in range(self.num_candidates):
            cand_id = f"CAND-{i+1:06d}"
            name = f"{random.choice(f_names)} {random.choice(l_names)}"
            email = f"{name.lower().replace(' ', '')}{i}@synthetic-drive.io"
            
            cand_records.append({"candidate_id": cand_id, "full_name": name, "email": email})
            
            # Assign deterministic skills profiles to candidate bridge tables
            chosen_skills = random.sample(skill_ids, random.randint(2, 4))
            for sk_id in chosen_skills:
                bridge_records.append({"candidate_id": cand_id, "skill_id": sk_id})
                
        return pd.DataFrame(cand_records), pd.DataFrame(bridge_records)

    # -------------------------------------------------------------------------
    # 2. Pipeline Segment: Dependent Transactional Layers
    # -------------------------------------------------------------------------
    def generate_applications(self, df_req: pd.DataFrame, df_cand: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        logger.info("Building core applications entry logs...")
        req_ids = df_req["requisition_id"].tolist()
        cand_ids = df_cand["candidate_id"].tolist()
        
        app_records = []
        resume_records = []
        seen_edges = set()
        res_counter = 1
        
        while len(app_records) < self.num_apps_target - 5:
            r_id = random.choice(req_ids)
            c_id = random.choice(cand_ids)
            
            if (r_id, c_id) not in seen_edges:
                seen_edges.add((r_id, c_id))
                app_id = f"APP-{r_id}-{c_id}"
                applied_date = self.base_date + datetime.timedelta(days=random.randint(1, 150))
                
                prob = random.random()
                if prob < 0.45: status = "SCREENING_FAILED"
                elif prob < 0.60: status = "WITHDRAWN"
                elif prob < 0.85: status = "INTERVIEW_STAGE"
                else: status = "OFFER_EXTENDED"
                
                app_records.append({
                    "application_id": app_id, "requisition_id": r_id, "candidate_id": c_id,
                    "status": status, "applied_date": applied_date
                })
                
                resume_records.append({
                    "resume_id": f"RES-{res_counter:06d}", "application_id": app_id,
                    "parsed_text_summary": f"Automated semantic resume data profile extraction for reference: {app_id}"
                })
                res_counter += 1
                
        return pd.DataFrame(app_records), pd.DataFrame(resume_records)

    def generate_interviews_and_feedback(self, df_app: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        logger.info("Assembling interviewer directories and technical execution logs...")
        
        interviewer_names = ["Erlang Shen", "Ada Lovelace", "Alan Turing", "Grace Hopper", "Linus Torvalds"]
        intv_records = [{"interviewer_id": f"INTV-{i+1:06d}", "name": name, "is_available": True} for i, name in enumerate(interviewer_names)]
        df_intv = pd.DataFrame(intv_records)
        
        interview_records = []
        feedback_records = []
        int_counter = 1
        
        target_apps = df_app[df_app["status"].isin(["INTERVIEW_STAGE", "OFFER_EXTENDED"])].to_dict("records")
        intv_ids = df_intv["interviewer_id"].tolist()
        
        for _ in range(self.num_interviews_target - 5):
            if not target_apps: break
            app = random.choice(target_apps)
            int_id = f"INT-2026-{int_counter:06d}"
            selected_intv = random.choice(intv_ids)
            scheduled_time = app["applied_date"] + datetime.timedelta(days=random.randint(5, 12))
            
            interview_records.append({
                "interview_id": int_id, "application_id": app["application_id"],
                "interviewer_id": selected_intv, "scheduled_time": scheduled_time, "round_number": random.randint(1, 3)
            })
            
            feedback_records.append({
                "feedback_id": f"FB-{int_id}-{selected_intv}", "interview_id": int_id,
                "interviewer_id": selected_intv, "score": random.randint(3, 5),
                "comments": "Meets criteria metrics framework evaluation standard specifications."
            })
            int_counter += 1
            
        return df_intv, pd.DataFrame(interview_records), pd.DataFrame(feedback_records)

    def generate_offers(self, df_app: pd.DataFrame, df_req: pd.DataFrame) -> pd.DataFrame:
        logger.info("Drafting transactional corporate onboarding offers dataset...")
        offer_records = []
        offer_counter = 1
        
        offer_apps = df_app[df_app["status"] == "OFFER_EXTENDED"].to_dict("records")
        req_lookup = df_req.set_index("requisition_id").to_dict("index")
        
        for _ in range(self.num_offers_target - 5):
            if not offer_apps: break
            app = random.choice(offer_apps)
            req_bounds = req_lookup.get(app["requisition_id"], {"min_salary": 70000.0, "max_salary": 120000.0})
            
            off_id = f"OFF-2026-{offer_counter:06d}"
            salary_offered = float(random.randint(int(req_bounds["min_salary"]), int(req_bounds["max_salary"])))
            
            offer_records.append({
                "offer_id": off_id, "application_id": app["application_id"],
                "salary_offered": salary_offered, "is_approved": True,
                "status": random.choice(["ACCEPTED", "REJECTED"])
            })
            offer_counter += 1
            
        return pd.DataFrame(offer_records)

    # -------------------------------------------------------------------------
    # 3. Pipeline Segment: Explicit Ground Truth Anomaly Injectors
    # -------------------------------------------------------------------------
    def inject_required_edge_scenarios(self, data_dfs: Dict[str, pd.DataFrame]) -> None:
        logger.warning("Injecting labeled operational anomalies into relational pipelines...")
        
        # --- Scenario A: Interview mapped to an Unavailable Interviewer ---
        data_dfs["interviewers"].loc[data_dfs["interviewers"]["interviewer_id"] == "INTV-000001", "is_available"] = False
        target_app_a = data_dfs["applications"].iloc[0]["application_id"]
        int_id_a = f"INT-2026-999001"
        new_int_row = pd.DataFrame([{
            "interview_id": int_id_a, "application_id": target_app_a, "interviewer_id": "INTV-000001",
            "scheduled_time": datetime.datetime(2026, 8, 15, 10, 0), "round_number": 1
        }])
        data_dfs["interviews"] = pd.concat([data_dfs["interviews"], new_int_row], ignore_index=True)
        self.ground_truth_records.append({
            "entity_id": int_id_a, "anomaly_type": "INTERVIEW_UNAVAILABLE_INTERVIEWER",
            "description": "Critical scheduling mismatch conflict: Interview allocated to blocked interviewer timeline."
        })
        
        # --- Scenario B: Candidate records an explicit Duplicate Submission ---
        sample_cand = data_dfs["candidates"].iloc[0]["candidate_id"]
        sample_req = data_dfs["job_requisitions"].iloc[0]["requisition_id"]
        app_id_b = f"APP-{sample_req}-{sample_cand}-DUP"
        new_app_row = pd.DataFrame([{
            "application_id": app_id_b, "requisition_id": sample_req, "candidate_id": sample_cand,
            "status": "APPLIED", "applied_date": datetime.datetime(2026, 9, 1)
        }])
        data_dfs["applications"] = pd.concat([data_dfs["applications"], new_app_row], ignore_index=True)
        self.ground_truth_records.append({
            "entity_id": app_id_b, "anomaly_type": "DUPLICATE_APPLICATION",
            "description": "Integrity Alert: Candidate recorded dual system entry requests targeting individual workflow pipeline."
        })
        
        # --- Scenario C: Stage advanced bypassing mandatory verification checks ---
        sample_req_c = data_dfs["job_requisitions"].iloc[1]["requisition_id"]
        sample_cand_c = data_dfs["candidates"].iloc[1]["candidate_id"]
        app_id_c = f"APP-{sample_req_c}-{sample_cand_c}"
        int_id_c = "INT-2026-999002"
        new_app_c = pd.DataFrame([{
            "application_id": app_id_c, "requisition_id": sample_req_c, "candidate_id": sample_cand_c,
            "status": "OFFER_EXTENDED", "applied_date": datetime.datetime(2026, 2, 1)
        }])
        new_int_c = pd.DataFrame([{
            "interview_id": int_id_c, "application_id": app_id_c, "interviewer_id": "INTV-000002",
            "scheduled_time": datetime.datetime(2026, 2, 5), "round_number": 2
        }])
        data_dfs["applications"] = pd.concat([data_dfs["applications"], new_app_c], ignore_index=True)
        data_dfs["interviews"] = pd.concat([data_dfs["interviews"], new_int_c], ignore_index=True)
        self.ground_truth_records.append({
            "entity_id": app_id_c, "anomaly_type": "MISSING_FEEDBACK_STAGE_ADVANCED",
            "description": "Compliance Alert: Workflow progression state advanced bypassing missing operational scoring checks."
        })
        
        # --- Scenario D: Compensation structures violating budget rules ---
        sample_req_d = data_dfs["job_requisitions"].iloc[2]
        app_id_d = f"APP-{sample_req_d['requisition_id']}-{data_dfs['candidates'].iloc[2]['candidate_id']}"
        off_id_d = "OFF-2026-999001"
        out_of_bounds_salary = float(sample_req_d["max_salary"] + 65000.0)
        new_app_d = pd.DataFrame([{
            "application_id": app_id_d, "requisition_id": sample_req_d['requisition_id'],
            "candidate_id": data_dfs['candidates'].iloc[2]['candidate_id'], "status": "OFFER_EXTENDED",
            "applied_date": datetime.datetime(2026, 4, 1)
        }])
        new_off_d = pd.DataFrame([{
            "offer_id": off_id_d, "application_id": app_id_d, "salary_offered": out_of_bounds_salary,
            "is_approved": True, "status": "PENDING"
        }])
        data_dfs["applications"] = pd.concat([data_dfs["applications"], new_app_d], ignore_index=True)
        data_dfs["offers"] = pd.concat([data_dfs["offers"], new_off_d], ignore_index=True)
        self.ground_truth_records.append({
            "entity_id": off_id_d, "anomaly_type": "OFFER_SALARY_OUT_OF_BAND",
            "description": f"Audit Failure: Offer value ({out_of_bounds_salary}) violates approved limits ({sample_req_d['max_salary']})."
        })
        
        # --- Scenario E: Closed requisitions processing candidate flows ---
        closed_req_id = data_dfs["job_requisitions"][data_dfs["job_requisitions"]["status"] == "CLOSED"].iloc[0]["requisition_id"]
        app_id_e = f"APP-{closed_req_id}-{data_dfs['candidates'].iloc[3]['candidate_id']}"
        new_app_e = pd.DataFrame([{
            "application_id": app_id_e, "requisition_id": closed_req_id,
            "candidate_id": data_dfs['candidates'].iloc[3]['candidate_id'], "status": "INTERVIEW_STAGE",
            "applied_date": datetime.datetime(2026, 5, 1)
        }])
        data_dfs["applications"] = pd.concat([data_dfs["applications"], new_app_e], ignore_index=True)
        self.ground_truth_records.append({
            "entity_id": closed_req_id, "anomaly_type": "CLOSED_ROLE_ACTIVE_APPLICATIONS",
            "description": "Lifecycle Paradox: Requisition closed system state contains active candidate routing sequences."
        })

    # -------------------------------------------------------------------------
    # 4. Pipeline Execution Orchestrator
    # -------------------------------------------------------------------------
    def build_complete_package(self) -> Dict[str, pd.DataFrame]:
        df_dept, df_skill = self.generate_master_metadata()
        df_req = self.generate_job_requisitions(df_dept)
        df_cand, df_cand_skill = self.generate_candidates(df_skill)
        df_app, df_resume = self.generate_applications(df_req, df_cand)
        df_intv, df_int, df_fb = self.generate_interviews_and_feedback(df_app)
        df_off = self.generate_offers(df_app, df_req)
        
        package = {
            "departments": df_dept, "skills": df_skill, "job_requisitions": df_req,
            "candidates": df_cand, "candidate_skills": df_cand_skill, "applications": df_app,
            "resume_artifacts": df_resume, "interviewers": df_intv, "interviews": df_int,
            "feedback": df_fb, "offers": df_off
        }
        
        self.inject_required_edge_scenarios(package)
        package["ground_truth"] = pd.DataFrame(self.ground_truth_records)
        return package


# -------------------------------------------------------------------------
# 5. Production Testing and Verification Routines
# -------------------------------------------------------------------------
def run_integrity_audits(data_dict: Dict[str, pd.DataFrame]) -> None:
    logger.info("Executing automated database primary-key and integrity constraint checking...")
    assert data_dict["job_requisitions"]["requisition_id"].is_unique, "PK violation detected on Job Requisitions table."
    assert data_dict["candidates"]["candidate_id"].is_unique, "PK violation detected on Candidates table."
    assert data_dict["applications"]["application_id"].is_unique, "PK violation detected on Applications table."
    assert data_dict["applications"]["requisition_id"].isin(data_dict["job_requisitions"]["requisition_id"]).all(), "FK constraints error: Applications mapping to invalid Job Requisitions."
    assert data_dict["applications"]["candidate_id"].isin(data_dict["candidates"]["candidate_id"]).all(), "FK constraints error: Applications mapping to invalid Candidate targets."
    logger.info("✅ Comprehensive relational schema constraint audits completed successfully.")


if __name__ == "__main__":
    export_directory = "./output_data"
    os.makedirs(export_directory, exist_ok=True)
    
    logger.info("Initiating Reproducibility Phase Verification...")
    pipeline_engine_a = ATSDatasetGenerator(scale_factor=1.0, seed=20261022)
    dataset_run1 = pipeline_engine_a.build_complete_package()
    
    pipeline_engine_b = ATSDatasetGenerator(scale_factor=1.0, seed=20261022)
    dataset_run2 = pipeline_engine_b.build_complete_package()
    
    run_1_checksums = {k: ATSDatasetGenerator.calculate_dataframe_checksum(v) for k, v in dataset_run1.items()}
    run_2_checksums = {k: ATSDatasetGenerator.calculate_dataframe_checksum(v) for k, v in dataset_run2.items()}
    
    if run_1_checksums == run_2_checksums:
        logger.info("⚡ Parity Check Passed: Data generator engine execution is completely deterministic.")
    else:
        logger.error("❌ Parity Check Failed: Multi-run divergence discovered inside output models.")
        raise AssertionError("Non-deterministic paths tracking anomaly pipelines.")
        
    run_integrity_audits(dataset_run1)
    
    logger.info(f"Exporting clean seed tables to '{export_directory}/' storage arrays...")
    for table_name, dataframe in dataset_run1.items():
        dataframe.to_csv(os.path.join(export_directory, f"{table_name}.csv"), index=False)
        
    logger.info("🏁 Challenge dataset generator run sequences executed perfectly!")
