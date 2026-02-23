"""Tests for genetic data validators."""
from __future__ import annotations

from healthbot.ingest.genetic_parser import (
    GeneticParser,
    GeneticVariant,
    validate_chromosome,
    validate_genotype,
    validate_position,
    validate_rsid,
)


class TestValidateRsid:
    """rsID format validation."""

    def test_valid_rsid(self):
        assert validate_rsid("rs1234567")

    def test_valid_internal_id(self):
        assert validate_rsid("i1234567")

    def test_invalid_no_prefix(self):
        assert not validate_rsid("1234567")

    def test_invalid_empty(self):
        assert not validate_rsid("")

    def test_invalid_letters_after_prefix(self):
        assert not validate_rsid("rsABC")


class TestValidateGenotype:
    """Genotype format validation."""

    def test_valid_heterozygous(self):
        assert validate_genotype("AG")

    def test_valid_homozygous(self):
        assert validate_genotype("CC")

    def test_valid_single_allele(self):
        assert validate_genotype("A")

    def test_valid_deletion(self):
        assert validate_genotype("D")

    def test_valid_insertion(self):
        assert validate_genotype("DI")

    def test_invalid_three_chars(self):
        assert not validate_genotype("AGC")

    def test_invalid_number(self):
        assert not validate_genotype("12")

    def test_case_insensitive(self):
        assert validate_genotype("ag")


class TestValidateChromosome:
    """Chromosome validation."""

    def test_autosome(self):
        assert validate_chromosome("1")
        assert validate_chromosome("22")

    def test_sex_chromosomes(self):
        assert validate_chromosome("X")
        assert validate_chromosome("Y")

    def test_mitochondrial(self):
        assert validate_chromosome("MT")

    def test_invalid_zero(self):
        assert not validate_chromosome("0")

    def test_invalid_number(self):
        assert not validate_chromosome("23")

    def test_invalid_text(self):
        assert not validate_chromosome("chr1")


class TestValidatePosition:
    """Genomic position validation."""

    def test_valid_position(self):
        assert validate_position(12345)

    def test_invalid_zero(self):
        assert not validate_position(0)

    def test_invalid_negative(self):
        assert not validate_position(-1)


class TestParserValidation:
    """Parser rejects invalid data with validators."""

    def test_invalid_chromosome_rejected(self):
        parser = GeneticParser()
        # chr0 is not valid
        data = "rsid\tchromosome\tposition\tgenotype\nrs123\t0\t100\tAG"
        result = parser.parse(data)
        assert len(result.variants) == 0
        assert result.skipped_lines == 1

    def test_invalid_rsid_rejected(self):
        parser = GeneticParser()
        data = "rsid\tchromosome\tposition\tgenotype\nBADID\t1\t100\tAG"
        result = parser.parse(data)
        assert len(result.variants) == 0

    def test_valid_data_accepted(self):
        parser = GeneticParser()
        data = "rsid\tchromosome\tposition\tgenotype\nrs123\t1\t100\tAG"
        result = parser.parse(data)
        assert len(result.variants) == 1
        assert result.variants[0].rsid == "rs123"
        assert result.variants[0].genotype == "AG"


class TestGenotypeConflictDetection:
    """Detect conflicting genotypes on re-import."""

    def test_no_conflict_same_genotype(self):
        existing = [GeneticVariant("rs123", "1", 100, "AG")]
        new = [GeneticVariant("rs123", "1", 100, "AG")]
        conflicts = GeneticParser.detect_conflicts(existing, new)
        assert len(conflicts) == 0

    def test_no_conflict_reversed_alleles(self):
        """AG and GA are the same genotype (allele order doesn't matter)."""
        existing = [GeneticVariant("rs123", "1", 100, "AG")]
        new = [GeneticVariant("rs123", "1", 100, "GA")]
        conflicts = GeneticParser.detect_conflicts(existing, new)
        assert len(conflicts) == 0

    def test_conflict_different_genotype(self):
        existing = [GeneticVariant("rs123", "1", 100, "AG")]
        new = [GeneticVariant("rs123", "1", 100, "CC")]
        conflicts = GeneticParser.detect_conflicts(existing, new)
        assert len(conflicts) == 1
        assert conflicts[0].rsid == "rs123"
        assert conflicts[0].existing_genotype == "AG"
        assert conflicts[0].new_genotype == "CC"

    def test_no_conflict_different_rsids(self):
        existing = [GeneticVariant("rs100", "1", 100, "AG")]
        new = [GeneticVariant("rs200", "1", 200, "CC")]
        conflicts = GeneticParser.detect_conflicts(existing, new)
        assert len(conflicts) == 0

    def test_multiple_conflicts(self):
        existing = [
            GeneticVariant("rs1", "1", 100, "AG"),
            GeneticVariant("rs2", "2", 200, "TT"),
        ]
        new = [
            GeneticVariant("rs1", "1", 100, "CC"),
            GeneticVariant("rs2", "2", 200, "AA"),
        ]
        conflicts = GeneticParser.detect_conflicts(existing, new)
        assert len(conflicts) == 2
