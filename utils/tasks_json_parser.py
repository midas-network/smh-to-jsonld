import json
import os
from typing import Dict, List, Any, Optional, Union
from dataclasses import dataclass


@dataclass
class TaskIdConfig:
    """Represents a task ID configuration with required and optional values."""
    required: Optional[List[Any]]
    optional: Optional[List[Any]]


@dataclass
class OutputTypeConfig:
    """Represents an output type configuration."""
    output_type_id: Any
    value: Dict[str, Any]


@dataclass
class TargetMetadata:
    """Represents metadata for a target."""
    target_id: str
    target_name: str
    uri: str
    description: Optional[str] = None
    target_units: Optional[str] = None
    target_keys: Optional[Dict[str, List[str]]] = None
    target_type: Optional[str] = None
    is_step_ahead: Optional[bool] = None
    time_unit: Optional[str] = None



@dataclass
class ModelTask:
    """Represents a model task configuration."""
    task_ids: Dict[str, TaskIdConfig]
    output_type: Dict[str, OutputTypeConfig]
    target_metadata: List[TargetMetadata]


@dataclass
class Round:
    """Represents a round configuration."""
    round_id: str
    round_id_from_variable: bool
    model_tasks: List[ModelTask]
    submissions_due: Dict[str, str]


class TasksConfig:
    """Class to read and access tasks configuration."""

    def __init__(self, file_path: str):
        """Initialize with tasks.json file path."""
        self.file_path = file_path
        self.schema_version = None
        self.rounds = []
        self._load_config()

    def _load_config(self):
        """Load configuration from the JSON file."""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"Tasks config file not found: {self.file_path}")

        try:
            with open(self.file_path, 'r') as f:
                data = json.load(f)

            self.schema_version = data.get("schema_version")

            # Parse rounds
            for round_data in data.get("rounds", []):
                model_tasks = []

                # Parse model tasks
                for task_data in round_data.get("model_tasks", []):
                    # Parse task IDs
                    task_ids = {}
                    for key, value in task_data.get("task_ids", {}).items():
                        task_ids[key] = TaskIdConfig(
                            required=value.get("required"),
                            optional=value.get("optional")
                        )

                    # Parse output types
                    output_type = {}
                    for key, value in task_data.get("output_type", {}).items():
                        output_type[key] = OutputTypeConfig(
                            output_type_id=value.get("output_type_id"),
                            value=value.get("value", {})
                        )

                    # Parse target metadata
                    target_metadata = []
                    for meta in task_data.get("target_metadata", []):
                        target_metadata.append(TargetMetadata(
                            target_id=meta.get("target_id"),
                            target_name=meta.get("target_name"),
                            uri=meta.get("uri"),
                            description=meta.get("description"),
                            target_units=meta.get("target_units"),
                            target_keys=meta.get("target_keys"),
                            target_type=meta.get("target_type"),
                            is_step_ahead=meta.get("is_step_ahead"),
                            time_unit=meta.get("time_unit"),
                        ))

                    model_tasks.append(ModelTask(
                        task_ids=task_ids,
                        output_type=output_type,
                        target_metadata=target_metadata
                    ))

                # Create round object
                round_dict = {}
                round_dict["round_id"] = None
                round_dict["model_tasks"] = model_tasks
                round_dict["submissions_due"] = round_data.get("submissions_due", {})

                round_dict["round_id_from_variable"] = round_data.get("round_id_from_variable")

                if round_dict["round_id_from_variable"]:
                    for model_task in model_tasks:
                        task_config = model_task.task_ids.get(round_data.get("round_id"))

                        if task_config and task_config.required:
                            if round_dict["round_id"] is None:
                                round_dict["round_id"] = task_config.required[0]
                            else:
                                if task_config.required[0] != round_dict["round_id"]:
                                    raise ValueError(
                                        f"Round ID mismatch: {round_dict['round_id']} and {task_config.required[0]}"
                                    )
                            break
                else:
                    round_dict["round_id"] = round_data.get("round_id")

                self.rounds.append(Round(
                    round_id=round_dict["round_id"],
                    round_id_from_variable=round_dict["round_id_from_variable"],
                    model_tasks=model_tasks,
                    submissions_due=round_dict["submissions_due"]
                ))

        except Exception as e:
            raise ValueError(f"Error parsing tasks config: {str(e)}")

    def get_all_rounds(self) -> List[Round]:
        """Get all rounds from the configuration."""
        return self.rounds

    def get_round_by_id(self, round_id: str) -> Optional[Round]:
        """Get a round by its ID."""
        for round_obj in self.rounds:
            if round_obj.round_id == round_id:
                return round_obj
        return None

    def get_latest_round(self) -> Optional[Round]:
        """Get the latest round (assuming the last one in the list is the latest)."""
        if self.rounds:
            return self.rounds[-1]
        return None

    def get_all_targets(self) -> List[str]:
        """Get a list of all unique target IDs across all rounds."""
        targets = set()
        for round_obj in self.rounds:
            for task in round_obj.model_tasks:
                for meta in task.target_metadata:
                    targets.add(meta.target_id)
        return list(targets)

    def get_all_locations(self) -> List[str]:
        """Get a list of all unique locations across all rounds."""
        locations = set()
        for round_obj in self.rounds:
            for task in round_obj.model_tasks:
                location_config = task.task_ids.get("location")
                if location_config:
                    if location_config.required:
                        locations.update(location_config.required)
                    if location_config.optional:
                        locations.update(location_config.optional)
        return list(locations)

    def get_all_values_for_task(self, task_ied: str) -> List[str]:
        """Get all values for a specific task ID across all rounds."""
        values = set()
        for round_obj in self.rounds:
            for task in round_obj.model_tasks:
                if task in task.task_ids:
                    task_config = task.task_ids[task]
                    if task_config.required:
                        values.update(task_config.required)
                    if task_config.optional:
                        values.update(task_config.optional)
        return list(values)

    def get_all_scenario_ids(self) -> List[str]:
        """Get a list of all unique scenario IDs across all rounds."""
        scenario_ids = set()
        for round_obj in self.rounds:
            for task in round_obj.model_tasks:
                task_config = task.task_ids.get("scenario_id")
                if task_config.required:
                    scenario_ids.update(task_config.required)
                if task_config.optional:
                    scenario_ids.update(task_config.optional)
        return list(scenario_ids)


def read_tasks_config(file_path: str = None) -> TasksConfig:
    """
    Read the tasks.json configuration file.

    Args:
        file_path: Path to the tasks.json file. If None, uses default path.

    Returns:
        TasksConfig object containing the parsed configuration.
    """
    if file_path is None:
        file_path = os.path.join('../data', 'hub-config', 'tasks.json')

    return TasksConfig(file_path)


if __name__ == "__main__":
    # Example usage
    config = read_tasks_config('../data/hub-config/tasks.json')
    rounds = config.get_all_rounds()
    for round_obj in rounds:
        print(f"Round ID: {round_obj.round_id}")
        for model_task in round_obj.model_tasks:
            print(f"  Model Task: {model_task.task_ids}")

    print(f"Schema version: {config.schema_version}")
    print(f"Number of rounds: {len(config.rounds)}")
    print(f"\nAll scenario IDs: {', '.join(config.get_all_scenario_ids())}")
    print(f"\nAll targets: {', '.join(config.get_all_targets())}")

    print(f"\nAll locations: {len(config.get_all_locations())} locations")
