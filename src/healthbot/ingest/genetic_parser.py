"""Parse raw genetic data files from TellMeGen, 23andMe, and AncestryDNA.

All supported formats share a common structure: tab/comma-separated rows
with rsID, chromosome, position, and genotype columns. Header lines and
comment lines (starting with #) are skipped.

The parser streams lines to handle large files (500K-700K variants typical).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger("healthbot")

# Valid genotype pattern: 1-2 letters from {A, C, G, T, D, I, -}
_GENOTYPE_RE = re.compile(r"^[ACGTDI\-]{1,2}$")

# Valid rsID pattern: "rs" + digits, or "i" + digits (internal IDs)
_RSID_RE = re.compile(r"^(rs|i)\d+$")

# Valid chromosomes
_VALID_CHROMOSOMES = {
    "1", "2", "3", "4", "5", "6", "7", "8", "9", "10",
    "11", "12", "13", "14", "15", "16", "17", "18", "19", "20",
    "21", "22", "X", "Y", "MT",
}


def validate_rsid(rsid: str) -> bool:
    """Validate an rsID format: 'rs' or 'i' followed by digits."""
    return bool(_RSID_RE.match(rsid))


def validate_genotype(genotype: str) -> bool:
    """Validate a genotype: 1-2 chars from {A, C, G, T, D, I, -}."""
    return bool(_GENOTYPE_RE.match(genotype.upper()))


def validate_chromosome(chromosome: str) -> bool:
    """Validate a chromosome: 1-22, X, Y, or MT."""
    return chromosome.upper() in _VALID_CHROMOSOMES


def validate_position(position: int) -> bool:
    """Validate a genomic position: must be a positive integer."""
    return isinstance(position, int) and position > 0

# Header patterns for format detection
_TELLMEGEN_HEADER = re.compile(r"rsid\s+chromosome\s+position\s+genotype", re.IGNORECASE)
_23ANDME_HEADER = re.compile(r"rsid\s+chromosome\s+position\s+genotype", re.IGNORECASE)
_ANCESTRY_HEADER = re.compile(r"rsid\s+chromosome\s+position\s+allele1\s+allele2", re.IGNORECASE)


@dataclass
class GeneticVariant:
    """A single genetic variant (SNP)."""

    rsid: str          # e.g., "rs1234567"
    chromosome: str    # e.g., "1", "X", "MT"
    position: int      # genomic position
    genotype: str      # e.g., "AG", "CC", "AT"
    source: str = "tellmegen"


@dataclass
class GenotypeConflict:
    """A conflicting genotype detected on re-import."""

    rsid: str
    existing_genotype: str
    new_genotype: str


@dataclass
class ParseResult:
    """Result of parsing a genetic data file."""

    variants: list[GeneticVariant]
    source: str
    total_lines: int
    skipped_lines: int
    warnings: list[str]
    conflicts: list[GenotypeConflict] | None = None


class GeneticParser:
    """Parse raw genetic data files from consumer genomics providers."""

    def parse(
        self, data: bytes | str, source: str = "auto",
    ) -> ParseResult:
        """Parse raw genetic data file.

        Args:
            data: File contents as bytes or string.
            source: Provider name ("tellmegen", "23andme", "ancestry", "auto").
                    "auto" detects from file headers.

        Returns:
            ParseResult with variants and parse stats.
        """
        if isinstance(data, bytes):
            data = data.decode("utf-8", errors="replace")

        lines = data.splitlines()
        detected_source = self._detect_format(lines) if source == "auto" else source
        is_ancestry = detected_source == "ancestry"

        variants: list[GeneticVariant] = []
        warnings: list[str] = []
        skipped = 0
        total = 0

        for line in lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Skip header lines
            if line.lower().startswith("rsid"):
                continue

            total += 1
            parts = re.split(r"[\t,]+", line)

            if is_ancestry:
                variant = self._parse_ancestry_line(parts, detected_source)
            else:
                variant = self._parse_standard_line(parts, detected_source)

            if variant is None:
                skipped += 1
                if skipped <= 5:
                    warnings.append(f"Skipped malformed line: {line[:80]}")
                continue

            variants.append(variant)

        if skipped > 5:
            warnings.append(f"... and {skipped - 5} more malformed lines")

        logger.info(
            "Parsed %d variants from %s (%d lines, %d skipped)",
            len(variants), detected_source, total, skipped,
        )
        return ParseResult(
            variants=variants,
            source=detected_source,
            total_lines=total,
            skipped_lines=skipped,
            warnings=warnings,
        )

    def _detect_format(self, lines: list[str]) -> str:
        """Detect file format from header/comment lines."""
        for line in lines[:50]:
            line = line.strip()
            if not line:
                continue
            if _ANCESTRY_HEADER.search(line):
                return "ancestry"
            if _TELLMEGEN_HEADER.search(line) or _23ANDME_HEADER.search(line):
                # TellMeGen and 23andMe use the same format
                if "tellmegen" in line.lower() or "tell me gen" in line.lower():
                    return "tellmegen"
                if "23andme" in line.lower():
                    return "23andme"
                return "tellmegen"  # Default for standard format
        return "unknown"

    def _parse_standard_line(
        self, parts: list[str], source: str,
    ) -> GeneticVariant | None:
        """Parse a standard format line (TellMeGen, 23andMe).

        Expected: rsid, chromosome, position, genotype
        """
        if len(parts) < 4:
            return None

        rsid = parts[0].strip()
        if not validate_rsid(rsid):
            return None

        chromosome = parts[1].strip()
        if not validate_chromosome(chromosome):
            return None

        try:
            position = int(parts[2].strip())
        except (ValueError, IndexError):
            return None
        if not validate_position(position):
            return None

        genotype = parts[3].strip().upper()
        if not validate_genotype(genotype):
            return None

        return GeneticVariant(
            rsid=rsid,
            chromosome=chromosome,
            position=position,
            genotype=genotype,
            source=source,
        )

    def _parse_ancestry_line(
        self, parts: list[str], source: str,
    ) -> GeneticVariant | None:
        """Parse AncestryDNA format line.

        Expected: rsid, chromosome, position, allele1, allele2
        """
        if len(parts) < 5:
            return None

        rsid = parts[0].strip()
        if not validate_rsid(rsid):
            return None

        chromosome = parts[1].strip()
        if not validate_chromosome(chromosome):
            return None

        try:
            position = int(parts[2].strip())
        except (ValueError, IndexError):
            return None
        if not validate_position(position):
            return None

        allele1 = parts[3].strip().upper()
        allele2 = parts[4].strip().upper()
        genotype = allele1 + allele2

        if not validate_genotype(genotype):
            return None

        return GeneticVariant(
            rsid=rsid,
            chromosome=chromosome,
            position=position,
            genotype=genotype,
            source=source,
        )

    @staticmethod
    def detect_conflicts(
        existing: list[GeneticVariant],
        new: list[GeneticVariant],
    ) -> list[GenotypeConflict]:
        """Detect conflicting genotypes between existing and new variant sets.

        Compares rsIDs and flags cases where the same rsID has a different
        genotype (accounting for allele order: AG == GA).
        """
        existing_map: dict[str, str] = {}
        for v in existing:
            existing_map[v.rsid] = v.genotype

        conflicts: list[GenotypeConflict] = []
        for v in new:
            if v.rsid in existing_map:
                old_gt = existing_map[v.rsid]
                new_gt = v.genotype
                # Normalize allele order for comparison (AG == GA)
                old_sorted = "".join(sorted(old_gt))
                new_sorted = "".join(sorted(new_gt))
                if old_sorted != new_sorted:
                    conflicts.append(GenotypeConflict(
                        rsid=v.rsid,
                        existing_genotype=old_gt,
                        new_genotype=new_gt,
                    ))
        return conflicts
