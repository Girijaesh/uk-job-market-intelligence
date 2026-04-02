"""
Tests for the NLP skill extraction module.
"""

from __future__ import annotations

import pytest

from src.nlp.skill_extractor import _extract_skills_from_text
from src.nlp.skills_list import SKILL_TO_CATEGORY


class TestExtractSkillsFromText:
    """Unit tests for _extract_skills_from_text()."""

    def test_basic_extraction(self):
        """Canonical skills are found in a realistic job description."""
        text = "We are looking for a python developer with pytorch experience."
        skills = [s for s, _ in _extract_skills_from_text(text)]
        assert "python" in skills
        assert "pytorch" in skills

    def test_r_not_extracted_from_docker(self):
        """The single-letter skill 'r' must not match inside the word 'docker'."""
        text = "Experience with docker and kubernetes required."
        skills = [s for s, _ in _extract_skills_from_text(text)]
        assert "r" not in skills

    def test_r_extracted_from_r_programming(self):
        """'r' should be found when it appears as a standalone word."""
        text = "Strong skills in R programming and statistical modelling."
        skills = [s for s, _ in _extract_skills_from_text(text)]
        assert "r" in skills

    def test_category_assignment(self):
        """Skills must be assigned their correct category from SKILL_TO_CATEGORY."""
        text = "python pytorch aws docker postgresql"
        pairs = {skill: cat for skill, cat in _extract_skills_from_text(text)}
        assert pairs.get("python") == "language"
        assert pairs.get("pytorch") == "ml_framework"
        assert pairs.get("aws") == "cloud"
        assert pairs.get("docker") == "devops"
        assert pairs.get("postgresql") == "database"

    def test_no_duplicates_per_description(self):
        """Each skill should appear at most once even if mentioned many times."""
        text = "python python python pytorch pytorch"
        pairs = _extract_skills_from_text(text)
        skills = [s for s, _ in pairs]
        assert len(skills) == len(set(skills))

    def test_case_insensitive(self):
        """Skill matching must be case-insensitive."""
        text = "PYTHON PYTORCH TensorFlow"
        skills = [s for s, _ in _extract_skills_from_text(text)]
        assert "python" in skills
        assert "pytorch" in skills
        assert "tensorflow" in skills

    def test_empty_string_returns_empty(self):
        """Empty or None input should return an empty list."""
        assert _extract_skills_from_text("") == []
        assert _extract_skills_from_text(None) == []  # type: ignore[arg-type]

    def test_multi_word_skills(self):
        """Multi-word skills like 'github actions' or 'scikit-learn' are found."""
        text = "CI/CD pipeline using github actions and scikit-learn for modelling"
        skills = [s for s, _ in _extract_skills_from_text(text)]
        assert "github actions" in skills
        assert "scikit-learn" in skills

    def test_mlops_skills(self):
        """MLOps-specific skills are categorised correctly."""
        text = "Experience with mlflow, kubeflow, and dvc for experiment tracking."
        pairs = {skill: cat for skill, cat in _extract_skills_from_text(text)}
        assert pairs.get("mlflow") == "mlops"
        assert pairs.get("kubeflow") == "mlops"
        assert pairs.get("dvc") == "mlops"

    def test_nlp_skills(self):
        """NLP skills including multi-word variants are found."""
        text = "Worked on RAG pipelines using LangChain and HuggingFace transformers."
        skills = [s for s, _ in _extract_skills_from_text(text)]
        assert "rag" in skills
        assert "langchain" in skills
        assert "huggingface" in skills

    def test_skill_to_category_coverage(self):
        """Every skill in SKILL_TO_CATEGORY must have a non-empty category."""
        for skill, category in SKILL_TO_CATEGORY.items():
            assert category, f"Skill '{skill}' has no category."
            assert isinstance(category, str)
