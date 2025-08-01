from typing import List, Generator, Dict
from sqlalchemy.orm import aliased

from src.constants import Prefix, DataSourceName
from src.input_adapters.drug_central.drug_node import DrugCentralAdapter
from src.input_adapters.drug_central.tables import Structures, ActTableFull, Reference
from src.interfaces.input_adapter import InputAdapter
from src.models.datasource_version_info import DatasourceVersionInfo
from src.models.ligand import Ligand, ProteinLigandRelationship, ActivityDetails
from src.models.node import EquivalentId
from src.models.protein import Protein


class ProteinDrugEdgeAdapter(InputAdapter, DrugCentralAdapter):

    def get_datasource_name(self) -> DataSourceName:
        return DataSourceName.DrugCentral

    def get_version(self) -> DatasourceVersionInfo:
        return self.version_info

    def get_all(self) -> Generator[List[ProteinLigandRelationship], None, None]:
        actReference = aliased(Reference)
        moaReference = aliased(Reference)
        query = (
            self.get_session().query(
                Structures.id,
                ActTableFull.accession,
                ActTableFull.act_value,
                ActTableFull.act_type,
                ActTableFull.act_comment.label('comment'),
                ActTableFull.act_source,
                ActTableFull.moa_source,
                ActTableFull.moa.label('has_moa'),
                ActTableFull.action_type,
                actReference.pmid.label('act_pmid'),
                moaReference.pmid.label('moa_pmid')
            )
            .join(ActTableFull, Structures.id == ActTableFull.struct_id)
            .join(actReference, ActTableFull.act_ref_id == actReference.id, isouter=True)
            .join(moaReference, ActTableFull.moa_ref_id == moaReference.id, isouter=True)
            .filter(ActTableFull.organism == 'Homo sapiens')).distinct()

        query_results = query.all()
        protein_ligand_rels: Dict[str, ProteinLigandRelationship] = {}

        for res in query_results:
            end_node=Ligand(id=EquivalentId(res.id, type=Prefix.DrugCentral).id_str())
            activityDetails = ActivityDetails()
            for field in ['act_value', 'act_type', 'action_type', 'act_source', 'moa_source', 'act_pmid', 'moa_pmid', 'comment']:
                val = getattr(res, field)
                if val not in (None, '', [], {}):
                    if field == 'act_pmid':
                        setattr(activityDetails, 'act_pmids', [val])
                    else:
                        setattr(activityDetails, field, val)

            if res.has_moa == 1:
                activityDetails.has_moa = True

            accessions = res.accession.split('|')
            for accession in accessions:
                start_node=Protein(id=EquivalentId(accession, type=Prefix.UniProtKB).id_str())

                rel_id = f"{start_node.id}|{end_node.id}"
                if rel_id in protein_ligand_rels:
                    protein_ligand_rels[rel_id].details.append(activityDetails)
                else:
                    pro_lig_rel = ProteinLigandRelationship(
                        start_node=start_node,
                        end_node=end_node,
                        details=[activityDetails]
                    )
                    protein_ligand_rels[rel_id] = pro_lig_rel

        yield list(protein_ligand_rels.values())
