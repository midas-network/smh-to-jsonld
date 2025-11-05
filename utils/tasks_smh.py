import logging


# def get_distinct_values_for_task(hub_df, hub_schema, task_name, sort=True):
#     """
#     Extract all unique location values (both required and optional) from tasks.json.
#
#     Parameters:
#         config: The tasks.json configuration object
#
#     Returns:
#         list: A sorted list of unique location codes
#     """
#     # task_values = set()
#     # if pa.types.is_date32(schema.field(task_name).type):
#     #     field_values[task_name] = [dt.strftime('%Y-%m-%d') for dt in field_values[task_name]]
#     # if pa.types.is_integer(schema.field(task_name).type):
#     #     field_values[task_name] = [str(int(val)) for val in field_values[task_name]]
#
#     for round_config in config.get_all_rounds():
#         config_round_id = round_config.round_id
#         if config_round_id == round_id:
#             for task in round_config.model_tasks:
#                 # Access task_ids to get location information
#                 if hasattr(task, 'task_ids'):
#                     # Add required locations if they exist
#
#                     if task.task_ids[task_name].required is not None:
#                         for task_value in task.task_ids[task_name].required:
#                             str_task_value = str(task_value)
#                             if str_task_value in field_values[task_name]:
#                                 task_values.add(str_task_value)
#                             else:
#                                 logging.error("Skipping required " + task_name + " not in global field values:", task_value)
#
#                     # Add optional locations if they exist
#                     if task.task_ids[task_name].optional is not None:
#                         for task_value in task.task_ids[task_name].optional:
#                             str_task_value = str(task_value)
#                             if str_task_value in field_values[task_name]:
#                                 task_values.add(str_task_value)
#                             else:
#                                 logging.debug(f"Skipping optional " + task_name + " not in global field values:{task_value}")
#             if sort:
#                 return sorted(list(task_values))
#             else:
#                 return list(task_values)
#     return []

def get_target_metadata(config, round_id):
    """Extract target metadxrata from tasks.json for each target."""
    target_metadata = {}

    # Go through all rounds and tasks
    for round_config in config.get_all_rounds():
        config_round_id = round_config.round_id
        if config_round_id == round_id:
            for task in round_config.model_tasks:
                if hasattr(task, 'target_metadata'):
                    for target in task.target_metadata:
                        # Access attributes directly instead of using .get()
                        if hasattr(target, 'target_id') and target.target_id not in target_metadata:
                            # Check if URI exists and log it
                            if hasattr(target, 'uri'):
                                logging.debug(
                                    f"Round {round_id}: Found URI '{target.uri}' for target '{target.target_id}'")
                            else:
                                logging.error(
                                    f"Round {round_id}: No URI attribute found for target '{target.target_id}'")

                            # Store the target metadata
                            target_metadata[target.target_id] = target

    logging.debug(f"Extracted metadata for {len(target_metadata)} unique targets")
    return target_metadata

def get_targets(config, round_id, field_values_by_model):
    target_metadata_dict = get_target_metadata(config, round_id)

    target_obj_list = []
    for target in target_metadata_dict:
        if target in (field_values_by_model['target']) and len(field_values_by_model['target']) > 0:
            # Get rich metadata from tasks.json
            target_md = target_metadata_dict.get(target, None)

            target_obj = {
                "@type": "PropertyValue",
                "name": target_md.target_name if target_md and hasattr(target_md, "target_name") else target
            }

            # Add URI if available - this is the key part for including the URI
            if target_md and hasattr(target_md, "uri") and target_md.uri:
                target_obj["identifier"] = target_md.uri

            # Add alternative name if available
            if target_md and hasattr(target_md, "alternative_name") and target_md.alternative_name:
                target_obj["alternateName"] = target_md.alternative_name

            # Add description
            if target_md and hasattr(target_md, "description") and target_md.description:
                target_obj["description"] = target_md.description

            # Add units
            if target_md and hasattr(target_md, "target_units") and target_md.target_units:
                target_obj["unitText"] = target_md.target_units

            if target_md and hasattr(target_md, "target_id") and target_md.target_id:
                target_obj["target_id"] = target_md.target_id

            if target_md and hasattr(target_md, "target_type") and target_md.target_type:
                target_obj["target_type"] = target_md.target_type

            if target_md and hasattr(target_md, "target_keys") and target_md.target_keys:
                target_obj["target_keys"] = target_md.target_keys

            # Add time unit if step-ahead target
            if target_md and hasattr(target_md, "is_step_ahead") and target_md.is_step_ahead:
                if hasattr(target_md, "time_unit"):
                    target_obj["temporalUnit"] = target_md.time_unit

            target_obj_list.append(target_obj)
        else:
            logging.debug("Skipping target metadata for target:", target)

    return target_obj_list
