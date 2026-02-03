"""Markdown documentation generator."""

import logging
from datetime import datetime
from pathlib import Path

from ..base.models import (
    Database,
    Function,
    Procedure,
    Schema,
    Sequence,
    Synonym,
    Table,
    Trigger,
    UserDefinedType,
    View,
)
from ..config import ScraperConfig

logger = logging.getLogger(__name__)


class MarkdownGenerator:
    """Generates markdown documentation from extracted schema."""

    def __init__(self, config: ScraperConfig):
        self.config = config
        self.output_dir = config.output_dir

    def generate(self, database: Database) -> list[Path]:
        """Generate all markdown files for the database."""
        created_files: list[Path] = []

        self._create_directories()
        created_files.append(self._generate_database_readme(database))

        if database.tables:
            created_files.extend(self._generate_table_docs(database.tables))
        if database.views:
            created_files.extend(self._generate_view_docs(database.views))
        if database.procedures:
            created_files.extend(self._generate_procedure_docs(database.procedures))
        if database.functions:
            created_files.extend(self._generate_function_docs(database.functions))
        if database.triggers:
            created_files.extend(self._generate_trigger_docs(database.triggers))
        if database.types:
            created_files.extend(self._generate_type_docs(database.types))
        if database.sequences:
            created_files.extend(self._generate_sequence_docs(database.sequences))
        if database.synonyms:
            created_files.extend(self._generate_synonym_docs(database.synonyms))

        created_files.extend(self._generate_schema_docs(database))
        return created_files

    def _create_directories(self) -> None:
        """Create the output directory structure."""
        dirs = [
            self.output_dir,
            self.output_dir / "tables",
            self.output_dir / "views",
            self.output_dir / "procedures",
            self.output_dir / "functions",
            self.output_dir / "triggers",
            self.output_dir / "types",
            self.output_dir / "sequences",
            self.output_dir / "synonyms",
            self.output_dir / "schemas",
        ]
        for d in dirs:
            d.mkdir(parents=True, exist_ok=True)

    def _write_file(self, path: Path, content: str) -> Path:
        """Write content to a file."""
        if self.config.dry_run:
            logger.info(f"[DRY RUN] Would write: {path}")
        else:
            path.write_text(content, encoding="utf-8")
            logger.debug(f"Wrote: {path}")
        return path

    def _generate_database_readme(self, database: Database) -> Path:
        """Generate the main README with database overview."""
        lines = [
            f"# {database.name} Database Schema",
            "",
            f"*Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*",
            "",
            f"**Database Type:** {database.db_type}",
        ]

        if database.server:
            lines.append(f"**Server:** {database.server}")
        if database.version:
            lines.append(f"**Version:** {database.version}")
        lines.append("")

        lines.extend([
            "## Summary",
            "",
            "| Object Type | Count |",
            "|-------------|-------|",
            f"| Tables | {len(database.tables)} |",
            f"| Views | {len(database.views)} |",
            f"| Stored Procedures | {len(database.procedures)} |",
            f"| Functions | {len(database.functions)} |",
            f"| Triggers | {len(database.triggers)} |",
            f"| User-Defined Types | {len(database.types)} |",
            f"| Sequences | {len(database.sequences)} |",
            f"| Synonyms | {len(database.synonyms)} |",
            "",
        ])

        schema_names = sorted(set(
            [t.schema_name for t in database.tables] +
            [v.schema_name for v in database.views] +
            [p.schema_name for p in database.procedures] +
            [f.schema_name for f in database.functions]
        ))
        if schema_names:
            lines.extend(["## Schemas", ""])
            for schema in schema_names:
                lines.append(f"- [{schema}](schemas/{schema}.md)")
            lines.append("")

        lines.extend([
            "## Object Directories",
            "",
            "- [Tables](tables/README.md)",
            "- [Views](views/README.md)",
            "- [Stored Procedures](procedures/README.md)",
            "- [Functions](functions/README.md)",
            "- [Triggers](triggers/README.md)",
            "- [User-Defined Types](types/README.md)",
            "- [Sequences](sequences/README.md)",
            "- [Synonyms](synonyms/README.md)",
            "",
        ])

        return self._write_file(self.output_dir / "README.md", "\n".join(lines))

    def _generate_table_docs(self, tables: list[Table]) -> list[Path]:
        """Generate documentation for all tables."""
        files = []

        index_lines = [
            "# Tables",
            "",
            f"Total: {len(tables)} tables",
            "",
            "| Schema | Table | Rows | Description |",
            "|--------|-------|------|-------------|",
        ]
        for table in sorted(tables, key=lambda t: (t.schema_name, t.name)):
            desc = (table.description or "")[:50]
            if len(table.description or "") > 50:
                desc += "..."
            index_lines.append(
                f"| {table.schema_name} | [{table.name}]({table.schema_name}.{table.name}.md) "
                f"| {table.row_count:,} | {desc} |"
            )

        files.append(self._write_file(self.output_dir / "tables" / "README.md", "\n".join(index_lines)))

        for table in tables:
            files.append(self._generate_table_file(table))

        return files

    def _generate_table_file(self, table: Table) -> Path:
        """Generate documentation for a single table."""
        lines = [f"# {table.full_name}", ""]

        if table.description:
            lines.extend([table.description, ""])

        lines.extend([
            "## Statistics",
            "",
            f"- **Rows:** {table.row_count:,}",
            f"- **Total Space:** {table.total_space_kb:,} KB",
            "",
            "## Columns",
            "",
            "| Column | Type | Nullable | Default | Description |",
            "|--------|------|----------|---------|-------------|",
        ])
        for col in table.columns:
            nullable = "YES" if col.is_nullable else "NO"
            default = col.default_value or ""
            if col.is_identity:
                default = f"IDENTITY({col.identity_seed or 1},{col.identity_increment or 1})"
            if col.is_computed:
                default = f"COMPUTED: {col.computed_definition}"
            desc = col.description or ""
            lines.append(f"| {col.name} | {col.full_type} | {nullable} | {default} | {desc} |")
        lines.append("")

        if table.primary_key:
            pk = table.primary_key
            clustered = "CLUSTERED" if pk.is_clustered else "NONCLUSTERED"
            lines.extend([
                "## Primary Key",
                "",
                f"**{pk.name}** ({clustered})",
                "",
                "Columns: " + ", ".join(f"`{c}`" for c in pk.columns),
                "",
            ])

        if table.foreign_keys:
            lines.extend([
                "## Foreign Keys",
                "",
                "| Name | Columns | References | On Delete | On Update |",
                "|------|---------|------------|-----------|-----------|",
            ])
            for fk in table.foreign_keys:
                cols = ", ".join(fk.columns)
                ref = f"{fk.referenced_schema}.{fk.referenced_table}({', '.join(fk.referenced_columns)})"
                lines.append(f"| {fk.name} | {cols} | {ref} | {fk.on_delete} | {fk.on_update} |")
            lines.append("")

        non_pk_indexes = [idx for idx in table.indexes if not idx.is_primary_key]
        if non_pk_indexes:
            lines.extend([
                "## Indexes",
                "",
                "| Name | Type | Columns | Filter |",
                "|------|------|---------|--------|",
            ])
            for idx in non_pk_indexes:
                idx_type = []
                if idx.is_unique:
                    idx_type.append("UNIQUE")
                idx_type.append(idx.index_type)
                cols = ", ".join(idx.columns)
                filter_def = idx.filter_definition or ""
                lines.append(f"| {idx.name} | {' '.join(idx_type)} | {cols} | {filter_def} |")
            lines.append("")

        if table.check_constraints:
            lines.extend(["## Check Constraints", ""])
            for cc in table.check_constraints:
                lines.extend([f"### {cc.name}", "", "```sql", cc.definition, "```", ""])

        if table.foreign_keys or table.referenced_by:
            lines.extend(["## Relationships", ""])
            if table.foreign_keys:
                lines.append("### References (this table → other tables)")
                lines.append("")
                for fk in table.foreign_keys:
                    lines.append(f"- → [{fk.referenced_schema}.{fk.referenced_table}](../{fk.referenced_schema}.{fk.referenced_table}.md) via `{fk.name}`")
                lines.append("")
            if table.referenced_by:
                lines.append("### Referenced By (other tables → this table)")
                lines.append("")
                for ref_schema, ref_table, fk_name in table.referenced_by:
                    lines.append(f"- ← [{ref_schema}.{ref_table}](../{ref_schema}.{ref_table}.md) via `{fk_name}`")
                lines.append("")

        return self._write_file(
            self.output_dir / "tables" / f"{table.schema_name}.{table.name}.md",
            "\n".join(lines)
        )

    def _generate_view_docs(self, views: list[View]) -> list[Path]:
        """Generate documentation for all views."""
        files = []

        index_lines = [
            "# Views",
            "",
            f"Total: {len(views)} views",
            "",
            "| Schema | View | Materialized | Description |",
            "|--------|------|--------------|-------------|",
        ]
        for view in sorted(views, key=lambda v: (v.schema_name, v.name)):
            desc = (view.description or "")[:50]
            mat = "Yes" if view.is_materialized else "No"
            index_lines.append(
                f"| {view.schema_name} | [{view.name}]({view.schema_name}.{view.name}.md) | {mat} | {desc} |"
            )

        files.append(self._write_file(self.output_dir / "views" / "README.md", "\n".join(index_lines)))

        for view in views:
            files.append(self._generate_view_file(view))

        return files

    def _generate_view_file(self, view: View) -> Path:
        """Generate documentation for a single view."""
        lines = [f"# {view.full_name}", ""]

        if view.description:
            lines.extend([view.description, ""])

        if view.is_materialized:
            lines.extend(["*This is a materialized view.*", ""])

        lines.extend([
            "## Columns",
            "",
            "| Column | Type | Nullable | Description |",
            "|--------|------|----------|-------------|",
        ])
        for col in view.columns:
            nullable = "YES" if col.is_nullable else "NO"
            desc = col.description or ""
            lines.append(f"| {col.name} | {col.full_type} | {nullable} | {desc} |")
        lines.append("")

        if view.base_tables:
            lines.extend(["## Base Tables", ""])
            for bt in view.base_tables:
                lines.append(f"- {bt}")
            lines.append("")

        if view.definition:
            lines.extend(["## Definition", "", "```sql", view.definition, "```", ""])

        return self._write_file(
            self.output_dir / "views" / f"{view.schema_name}.{view.name}.md",
            "\n".join(lines)
        )

    def _generate_procedure_docs(self, procedures: list[Procedure]) -> list[Path]:
        """Generate documentation for all stored procedures."""
        files = []

        index_lines = [
            "# Stored Procedures",
            "",
            f"Total: {len(procedures)} stored procedures",
            "",
            "| Schema | Procedure | Parameters | Language |",
            "|--------|-----------|------------|----------|",
        ]
        for proc in sorted(procedures, key=lambda p: (p.schema_name, p.name)):
            param_count = len(proc.parameters)
            index_lines.append(
                f"| {proc.schema_name} | [{proc.name}]({proc.schema_name}.{proc.name}.md) "
                f"| {param_count} | {proc.language} |"
            )

        files.append(self._write_file(self.output_dir / "procedures" / "README.md", "\n".join(index_lines)))

        for proc in procedures:
            files.append(self._generate_procedure_file(proc))

        return files

    def _generate_procedure_file(self, proc: Procedure) -> Path:
        """Generate documentation for a single stored procedure."""
        lines = [f"# {proc.full_name}", "", f"**Language:** {proc.language}", ""]

        if proc.description:
            lines.extend([proc.description, ""])

        if proc.parameters:
            lines.extend([
                "## Parameters",
                "",
                "| Name | Type | Direction | Default |",
                "|------|------|-----------|---------|",
            ])
            for param in proc.parameters:
                direction = "OUTPUT" if param.is_output else "INPUT"
                default = param.default_value if param.has_default else ""
                lines.append(f"| {param.name} | {param.full_type} | {direction} | {default} |")
            lines.append("")
        else:
            lines.extend(["*No parameters*", ""])

        if proc.definition:
            lines.extend(["## Definition", "", "```sql", proc.definition, "```", ""])

        return self._write_file(
            self.output_dir / "procedures" / f"{proc.schema_name}.{proc.name}.md",
            "\n".join(lines)
        )

    def _generate_function_docs(self, functions: list[Function]) -> list[Path]:
        """Generate documentation for all functions."""
        files = []

        index_lines = [
            "# User-Defined Functions",
            "",
            f"Total: {len(functions)} functions",
            "",
            "| Schema | Function | Type | Parameters | Language |",
            "|--------|----------|------|------------|----------|",
        ]
        for func in sorted(functions, key=lambda f: (f.schema_name, f.name)):
            param_count = len(func.parameters)
            func_type = func.function_type.replace("_", " ").title()
            index_lines.append(
                f"| {func.schema_name} | [{func.name}]({func.schema_name}.{func.name}.md) "
                f"| {func_type} | {param_count} | {func.language} |"
            )

        files.append(self._write_file(self.output_dir / "functions" / "README.md", "\n".join(index_lines)))

        for func in functions:
            files.append(self._generate_function_file(func))

        return files

    def _generate_function_file(self, func: Function) -> Path:
        """Generate documentation for a single function."""
        func_type = func.function_type.replace("_", " ").title()
        lines = [
            f"# {func.full_name}",
            "",
            f"**Type:** {func_type}",
            f"**Language:** {func.language}",
            "",
        ]

        if func.description:
            lines.extend([func.description, ""])

        if func.parameters:
            lines.extend([
                "## Parameters",
                "",
                "| Name | Type | Default |",
                "|------|------|---------|",
            ])
            for param in func.parameters:
                default = param.default_value if param.has_default else ""
                lines.append(f"| {param.name} | {param.full_type} | {default} |")
            lines.append("")

        if func.function_type == "SCALAR" and func.return_type:
            lines.extend(["## Returns", "", f"`{func.return_type}`", ""])
        elif func.return_columns:
            lines.extend([
                "## Return Columns",
                "",
                "| Column | Type | Nullable |",
                "|--------|------|----------|",
            ])
            for col in func.return_columns:
                nullable = "YES" if col.is_nullable else "NO"
                lines.append(f"| {col.name} | {col.full_type} | {nullable} |")
            lines.append("")

        if func.definition:
            lines.extend(["## Definition", "", "```sql", func.definition, "```", ""])

        return self._write_file(
            self.output_dir / "functions" / f"{func.schema_name}.{func.name}.md",
            "\n".join(lines)
        )

    def _generate_trigger_docs(self, triggers: list[Trigger]) -> list[Path]:
        """Generate documentation for all triggers."""
        files = []

        index_lines = [
            "# Triggers",
            "",
            f"Total: {len(triggers)} triggers",
            "",
            "| Schema | Trigger | Table | Type | Events | Disabled |",
            "|--------|---------|-------|------|--------|----------|",
        ]
        for trigger in sorted(triggers, key=lambda t: (t.schema_name, t.name)):
            events = ", ".join(trigger.events)
            disabled = "Yes" if trigger.is_disabled else "No"
            index_lines.append(
                f"| {trigger.schema_name} | [{trigger.name}]({trigger.schema_name}.{trigger.name}.md) "
                f"| {trigger.parent_table_name} | {trigger.trigger_type} | {events} | {disabled} |"
            )

        files.append(self._write_file(self.output_dir / "triggers" / "README.md", "\n".join(index_lines)))

        for trigger in triggers:
            files.append(self._generate_trigger_file(trigger))

        return files

    def _generate_trigger_file(self, trigger: Trigger) -> Path:
        """Generate documentation for a single trigger."""
        lines = [
            f"# {trigger.full_name}",
            "",
            f"**Table:** [{trigger.parent_table_schema}.{trigger.parent_table_name}](../tables/{trigger.parent_table_schema}.{trigger.parent_table_name}.md)",
            "",
            f"**Type:** {trigger.trigger_type}",
            "",
            f"**Events:** {', '.join(trigger.events)}",
            "",
        ]

        if trigger.is_disabled:
            lines.extend(["*This trigger is disabled.*", ""])

        if trigger.description:
            lines.extend([trigger.description, ""])

        if trigger.definition:
            lines.extend(["## Definition", "", "```sql", trigger.definition, "```", ""])

        return self._write_file(
            self.output_dir / "triggers" / f"{trigger.schema_name}.{trigger.name}.md",
            "\n".join(lines)
        )

    def _generate_type_docs(self, types: list[UserDefinedType]) -> list[Path]:
        """Generate documentation for all user-defined types."""
        files = []

        index_lines = [
            "# User-Defined Types",
            "",
            f"Total: {len(types)} types",
            "",
            "| Schema | Type | Category | Base Type |",
            "|--------|------|----------|-----------|",
        ]
        for udt in sorted(types, key=lambda t: (t.schema_name, t.name)):
            category = udt.type_category.replace("_", " ").title()
            base = udt.base_type or "-"
            index_lines.append(
                f"| {udt.schema_name} | [{udt.name}]({udt.schema_name}.{udt.name}.md) | {category} | {base} |"
            )

        files.append(self._write_file(self.output_dir / "types" / "README.md", "\n".join(index_lines)))

        for udt in types:
            files.append(self._generate_type_file(udt))

        return files

    def _generate_type_file(self, udt: UserDefinedType) -> Path:
        """Generate documentation for a single user-defined type."""
        category = udt.type_category.replace("_", " ").title()
        lines = [f"# {udt.full_name}", "", f"**Category:** {category}", ""]

        if udt.description:
            lines.extend([udt.description, ""])

        if udt.base_type:
            nullable = "NULL" if udt.is_nullable else "NOT NULL"
            lines.extend(["## Definition", "", f"Base type: `{udt.base_type}` {nullable}", ""])

        if udt.columns:
            lines.extend([
                "## Columns",
                "",
                "| Column | Type | Nullable |",
                "|--------|------|----------|",
            ])
            for col in udt.columns:
                nullable = "YES" if col.is_nullable else "NO"
                lines.append(f"| {col.name} | {col.full_type} | {nullable} |")
            lines.append("")

        if udt.enum_values:
            lines.extend(["## Values", ""])
            for val in udt.enum_values:
                lines.append(f"- `{val}`")
            lines.append("")

        if udt.check_constraint:
            lines.extend(["## Check Constraint", "", "```sql", udt.check_constraint, "```", ""])

        return self._write_file(
            self.output_dir / "types" / f"{udt.schema_name}.{udt.name}.md",
            "\n".join(lines)
        )

    def _generate_sequence_docs(self, sequences: list[Sequence]) -> list[Path]:
        """Generate documentation for all sequences."""
        files = []

        index_lines = [
            "# Sequences",
            "",
            f"Total: {len(sequences)} sequences",
            "",
            "| Schema | Sequence | Type | Start | Increment | Current | Cycling |",
            "|--------|----------|------|-------|-----------|---------|---------|",
        ]
        for seq in sorted(sequences, key=lambda s: (s.schema_name, s.name)):
            cycling = "Yes" if seq.is_cycling else "No"
            current = seq.current_value if seq.current_value is not None else "-"
            index_lines.append(
                f"| {seq.schema_name} | [{seq.name}]({seq.schema_name}.{seq.name}.md) "
                f"| {seq.data_type} | {seq.start_value} | {seq.increment} | {current} | {cycling} |"
            )

        files.append(self._write_file(self.output_dir / "sequences" / "README.md", "\n".join(index_lines)))

        for seq in sequences:
            files.append(self._generate_sequence_file(seq))

        return files

    def _generate_sequence_file(self, seq: Sequence) -> Path:
        """Generate documentation for a single sequence."""
        lines = [f"# {seq.full_name}", ""]

        if seq.description:
            lines.extend([seq.description, ""])

        cycling = "Yes" if seq.is_cycling else "No"
        cache = seq.cache_size if seq.cache_size else "No cache"

        lines.extend([
            "## Properties",
            "",
            f"- **Data Type:** {seq.data_type}",
            f"- **Start Value:** {seq.start_value}",
            f"- **Increment:** {seq.increment}",
            f"- **Minimum Value:** {seq.min_value}",
            f"- **Maximum Value:** {seq.max_value}",
            f"- **Current Value:** {seq.current_value}",
            f"- **Cycling:** {cycling}",
            f"- **Cache:** {cache}",
            "",
        ])

        return self._write_file(
            self.output_dir / "sequences" / f"{seq.schema_name}.{seq.name}.md",
            "\n".join(lines)
        )

    def _generate_synonym_docs(self, synonyms: list[Synonym]) -> list[Path]:
        """Generate documentation for all synonyms."""
        files = []

        index_lines = [
            "# Synonyms",
            "",
            f"Total: {len(synonyms)} synonyms",
            "",
            "| Schema | Synonym | Target |",
            "|--------|---------|--------|",
        ]
        for syn in sorted(synonyms, key=lambda s: (s.schema_name, s.name)):
            index_lines.append(
                f"| {syn.schema_name} | [{syn.name}]({syn.schema_name}.{syn.name}.md) | {syn.base_object_name} |"
            )

        files.append(self._write_file(self.output_dir / "synonyms" / "README.md", "\n".join(index_lines)))

        for syn in synonyms:
            files.append(self._generate_synonym_file(syn))

        return files

    def _generate_synonym_file(self, syn: Synonym) -> Path:
        """Generate documentation for a single synonym."""
        lines = [f"# {syn.full_name}", ""]

        if syn.description:
            lines.extend([syn.description, ""])

        lines.extend(["## Target", "", f"**Base Object:** `{syn.base_object_name}`", ""])

        if syn.target_server or syn.target_database:
            lines.append("### Parsed Reference")
            lines.append("")
            if syn.target_server:
                lines.append(f"- **Server:** {syn.target_server}")
            if syn.target_database:
                lines.append(f"- **Database:** {syn.target_database}")
            if syn.target_schema:
                lines.append(f"- **Schema:** {syn.target_schema}")
            if syn.target_object:
                lines.append(f"- **Object:** {syn.target_object}")
            lines.append("")

        return self._write_file(
            self.output_dir / "synonyms" / f"{syn.schema_name}.{syn.name}.md",
            "\n".join(lines)
        )

    def _generate_schema_docs(self, database: Database) -> list[Path]:
        """Generate per-schema documentation."""
        files = []

        schemas: dict[str, Schema] = {}

        for table in database.tables:
            if table.schema_name not in schemas:
                schemas[table.schema_name] = Schema(name=table.schema_name)
            schemas[table.schema_name].tables.append(table)

        for view in database.views:
            if view.schema_name not in schemas:
                schemas[view.schema_name] = Schema(name=view.schema_name)
            schemas[view.schema_name].views.append(view)

        for proc in database.procedures:
            if proc.schema_name not in schemas:
                schemas[proc.schema_name] = Schema(name=proc.schema_name)
            schemas[proc.schema_name].procedures.append(proc)

        for func in database.functions:
            if func.schema_name not in schemas:
                schemas[func.schema_name] = Schema(name=func.schema_name)
            schemas[func.schema_name].functions.append(func)

        for trigger in database.triggers:
            if trigger.schema_name not in schemas:
                schemas[trigger.schema_name] = Schema(name=trigger.schema_name)
            schemas[trigger.schema_name].triggers.append(trigger)

        for udt in database.types:
            if udt.schema_name not in schemas:
                schemas[udt.schema_name] = Schema(name=udt.schema_name)
            schemas[udt.schema_name].types.append(udt)

        for seq in database.sequences:
            if seq.schema_name not in schemas:
                schemas[seq.schema_name] = Schema(name=seq.schema_name)
            schemas[seq.schema_name].sequences.append(seq)

        for syn in database.synonyms:
            if syn.schema_name not in schemas:
                schemas[syn.schema_name] = Schema(name=syn.schema_name)
            schemas[syn.schema_name].synonyms.append(syn)

        index_lines = [
            "# Schemas",
            "",
            f"Total: {len(schemas)} schemas",
            "",
            "| Schema | Tables | Views | Procedures | Functions |",
            "|--------|--------|-------|------------|-----------|",
        ]
        for name, schema in sorted(schemas.items()):
            index_lines.append(
                f"| [{name}]({name}.md) | {len(schema.tables)} | {len(schema.views)} "
                f"| {len(schema.procedures)} | {len(schema.functions)} |"
            )

        files.append(self._write_file(self.output_dir / "schemas" / "README.md", "\n".join(index_lines)))

        for schema in schemas.values():
            files.append(self._generate_schema_file(schema))

        return files

    def _generate_schema_file(self, schema: Schema) -> Path:
        """Generate documentation for a single schema."""
        lines = [f"# Schema: {schema.name}", ""]

        if schema.tables:
            lines.extend(["## Tables", ""])
            for table in sorted(schema.tables, key=lambda t: t.name):
                lines.append(f"- [{table.name}](../tables/{table.schema_name}.{table.name}.md)")
            lines.append("")

        if schema.views:
            lines.extend(["## Views", ""])
            for view in sorted(schema.views, key=lambda v: v.name):
                lines.append(f"- [{view.name}](../views/{view.schema_name}.{view.name}.md)")
            lines.append("")

        if schema.procedures:
            lines.extend(["## Stored Procedures", ""])
            for proc in sorted(schema.procedures, key=lambda p: p.name):
                lines.append(f"- [{proc.name}](../procedures/{proc.schema_name}.{proc.name}.md)")
            lines.append("")

        if schema.functions:
            lines.extend(["## Functions", ""])
            for func in sorted(schema.functions, key=lambda f: f.name):
                lines.append(f"- [{func.name}](../functions/{func.schema_name}.{func.name}.md)")
            lines.append("")

        if schema.triggers:
            lines.extend(["## Triggers", ""])
            for trigger in sorted(schema.triggers, key=lambda t: t.name):
                lines.append(f"- [{trigger.name}](../triggers/{trigger.schema_name}.{trigger.name}.md)")
            lines.append("")

        if schema.types:
            lines.extend(["## User-Defined Types", ""])
            for udt in sorted(schema.types, key=lambda t: t.name):
                lines.append(f"- [{udt.name}](../types/{udt.schema_name}.{udt.name}.md)")
            lines.append("")

        if schema.sequences:
            lines.extend(["## Sequences", ""])
            for seq in sorted(schema.sequences, key=lambda s: s.name):
                lines.append(f"- [{seq.name}](../sequences/{seq.schema_name}.{seq.name}.md)")
            lines.append("")

        if schema.synonyms:
            lines.extend(["## Synonyms", ""])
            for syn in sorted(schema.synonyms, key=lambda s: s.name):
                lines.append(f"- [{syn.name}](../synonyms/{syn.schema_name}.{syn.name}.md)")
            lines.append("")

        return self._write_file(
            self.output_dir / "schemas" / f"{schema.name}.md",
            "\n".join(lines)
        )
