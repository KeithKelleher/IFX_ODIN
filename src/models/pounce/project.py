from dataclasses import dataclass, field
from datetime import date
from typing import List

from src.interfaces.simple_enum import SimpleEnum
from src.models.node import Node, Relationship


class ProjectPrivacy(SimpleEnum):
    Private = "Private"
    NCATS = "NCATS"
    Public = "Public"


@dataclass
class ProjectType(Node):
    name: str = None


@dataclass
class Project(Node):
    name: str = None
    description: str = None
    lab_groups: List[str] = field(default_factory=list)
    date: date = None
    privacy_level: ProjectPrivacy = None
    keywords: List[str] = field(default_factory=list)


@dataclass
class ProjectTypeRelationship(Relationship):
    start_node: Project
    end_node: ProjectType
