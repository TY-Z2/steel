"""Schema validation and enrichment helpers for extracted steel data."""

from __future__ import annotations

import logging
from typing import Dict, Iterable, List, Optional, Tuple, Union

from pydantic import BaseModel, Field, ValidationError, validator


logger = logging.getLogger(__name__)


Number = Union[int, float]
KEY_ALLOY_ELEMENTS: Tuple[str, ...] = (
    "C",
    "Si",
    "Mn",
    "Cr",
    "Mo",
    "Ni",
    "V",
    "Ti",
    "Nb",
    "Cu",
    "B",
)


class CompositionModel(BaseModel):
    """Validate composition ranges."""

    __root__: Dict[str, float]

    @validator("__root__")
    def validate_values(cls, value: Dict[str, Number]) -> Dict[str, float]:
        clean: Dict[str, float] = {}
        for element, raw in value.items():
            try:
                numeric = float(raw)
            except (TypeError, ValueError) as exc:  # pragma: no cover - defensive
                raise ValueError(f"元素 {element} 不是数值: {raw}") from exc
            if not 0 <= numeric <= 100:
                raise ValueError(f"元素 {element} 取值 {numeric} 超出 0-100% 范围")
            clean[element] = round(numeric, 4)
        return clean

    def dict(self, **kwargs):  # type: ignore[override]
        return dict(self.__root__)


class QualityMetadata(BaseModel):
    """Metadata that accompanies each record for auditability."""

    confidence: float = Field(0.0, ge=0.0, le=1.0)
    source_page: Optional[int] = Field(default=None, ge=0)
    parsing_path: str = (
        "process_papers_from_directory -> process_pdf -> extract_steel_data_from_text"
    )
    issues: List[str] = Field(default_factory=list)
    manual_review: Optional[Dict[str, object]] = None


class DerivedMetrics(BaseModel):
    """Derived metrics computed from validated values."""

    total_key_alloy_content: Optional[float] = Field(default=None, ge=0, le=100)
    carbon_equivalent: Optional[float] = Field(default=None, ge=0)


class SteelRecord(BaseModel):
    """Schema for a validated steel record."""

    file_path: str
    composition: Dict[str, float] = Field(default_factory=dict)
    heat_treatment: Dict[str, Union[float, str]] = Field(default_factory=dict)
    mechanical_properties: Dict[str, float] = Field(default_factory=dict)
    microstructure: Dict[str, float] = Field(default_factory=dict)
    text_snippet: Optional[str] = None
    derived_metrics: DerivedMetrics = Field(default_factory=DerivedMetrics)
    quality_metadata: QualityMetadata = Field(default_factory=QualityMetadata)

    @validator("composition", pre=True, always=True)
    def validate_composition(cls, value: Dict[str, Number]) -> Dict[str, float]:
        if not value:
            return {}
        composition = CompositionModel(__root__=value)
        return composition.dict()

    @validator("heat_treatment", pre=True, always=True)
    def validate_heat_treatment(
        cls, value: Dict[str, Union[Number, str]], values
    ) -> Dict[str, Union[float, str]]:
        if not value:
            return {}

        cleaned: Dict[str, Union[float, str]] = {}
        issues: List[str] = []

        for key, raw in value.items():
            if raw is None:
                continue
            if key == "quenching_medium":
                cleaned[key] = str(raw).lower()
                continue

            try:
                numeric = float(raw)
            except (TypeError, ValueError):
                issues.append(f"热处理字段 {key} 不是数值: {raw}")
                continue

            if numeric < 0:
                issues.append(f"热处理字段 {key} 取值为负: {numeric}")
            if "temperature" in key and numeric > 1500:
                issues.append(f"热处理温度 {key} 超过 1500℃: {numeric}")

            cleaned[key] = round(numeric, 4)

        if issues:
            raise ValueError("; ".join(issues))

        return cleaned

    @validator("mechanical_properties", pre=True, always=True)
    def validate_mechanical_properties(cls, value: Dict[str, Number]) -> Dict[str, float]:
        if not value:
            return {}
        cleaned: Dict[str, float] = {}
        for key, raw in value.items():
            try:
                numeric = float(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"力学性能字段 {key} 不是数值: {raw}") from exc
            if numeric < 0:
                raise ValueError(f"力学性能字段 {key} 为负值: {numeric}")
            cleaned[key] = round(numeric, 4)
        return cleaned

    @validator("microstructure", pre=True, always=True)
    def validate_microstructure(cls, value: Dict[str, Number]) -> Dict[str, float]:
        if not value:
            return {}
        cleaned: Dict[str, float] = {}
        for key, raw in value.items():
            try:
                numeric = float(raw)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"显微组织字段 {key} 不是数值: {raw}") from exc
            if not 0 <= numeric <= 100:
                raise ValueError(f"显微组织字段 {key} 超出 0-100% 范围: {numeric}")
            cleaned[key] = round(numeric, 4)
        return cleaned


def compute_confidence_score(record: Dict[str, Dict[str, object]]) -> float:
    """Heuristic confidence based on population of major sections."""

    sections = [
        record.get("composition"),
        record.get("heat_treatment"),
        record.get("mechanical_properties"),
        record.get("microstructure"),
    ]
    total = len(sections)
    filled = sum(1 for section in sections if section)
    return round(filled / total if total else 0.0, 3)


def compute_derived_metrics(composition: Dict[str, float]) -> DerivedMetrics:
    """Compute derived metrics using validated composition."""

    total_key_alloy = sum(composition.get(element, 0.0) for element in KEY_ALLOY_ELEMENTS)
    carbon_equivalent = None

    if composition:
        c = composition.get("C", 0.0)
        mn = composition.get("Mn", 0.0)
        cr = composition.get("Cr", 0.0)
        mo = composition.get("Mo", 0.0)
        v = composition.get("V", 0.0)
        ni = composition.get("Ni", 0.0)
        cu = composition.get("Cu", 0.0)
        carbon_equivalent = c + mn / 6 + (cr + mo + v) / 5 + (ni + cu) / 15

    return DerivedMetrics(
        total_key_alloy_content=round(total_key_alloy, 4) if total_key_alloy else None,
        carbon_equivalent=round(carbon_equivalent, 4) if carbon_equivalent else None,
    )


def enrich_record(record: Dict[str, object]) -> Dict[str, object]:
    """Add derived metrics and metadata prior to validation."""

    enriched = dict(record)
    composition = enriched.get("composition") or {}

    derived = compute_derived_metrics(composition)
    issues: List[str] = []
    for section in ("composition", "heat_treatment"):
        if not enriched.get(section):
            issues.append(f"缺少关键字段: {section}")

    metadata = QualityMetadata(
        confidence=compute_confidence_score(enriched),
        source_page=enriched.get("source_page"),
        issues=issues,
    )

    enriched["derived_metrics"] = derived.dict()
    enriched["quality_metadata"] = metadata.dict()

    return enriched


def validate_record(record: Dict[str, object]) -> SteelRecord:
    """Validate a single raw record."""

    enriched = enrich_record(record)
    return SteelRecord(**enriched)


def validate_dataset(dataset: Iterable[Dict[str, object]]) -> Tuple[List[Dict[str, object]], List[str]]:
    """Validate all records and collect validation errors."""

    validated: List[Dict[str, object]] = []
    errors: List[str] = []

    for index, record in enumerate(dataset):
        try:
            validated_record = validate_record(record)
        except ValidationError as exc:
            message = f"记录 {index} 未通过校验: {exc}"
            logger.warning(message)
            errors.append(message)
            continue

        validated.append(validated_record.dict())

    return validated, errors


__all__ = [
    "DerivedMetrics",
    "QualityMetadata",
    "SteelRecord",
    "compute_confidence_score",
    "compute_derived_metrics",
    "enrich_record",
    "validate_dataset",
]

