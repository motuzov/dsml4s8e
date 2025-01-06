from dagster import (
    Config,
    OpExecutionContext,
    RunConfig,
    config_mapping,
    job,
    op,
    Field,
)


class DoSomethingConfig(Config):
    config_param: str


class Op1Config(Config):
    a: str


@op(
    config_schema={"a": str},
)
def op1(context: OpExecutionContext) -> str:
    # context.log.info("config_param op1: " + config["a"])
    context.log.info(context.op_config)
    return "op1 msg"


@op
def op2(context: OpExecutionContext, config: DoSomethingConfig, op1) -> None:
    context.log.info("config_param op2: " + config.config_param)
    context.log.info("op2 op1 msg: " + op1)


class SimplifiedConfig(Config):
    simplified_param: str
    a: str


@config_mapping
def simplified_config(val: SimplifiedConfig) -> RunConfig:
    return RunConfig(
        ops={
            "op1": Op1Config(a=val.a),
            "op2": DoSomethingConfig(config_param=val.simplified_param),
        }
    )


@job(config=simplified_config)
# @job
def do_it_all_with_simplified_config():
    op2(op1())


if __name__ == "__main__":
    do_it_all_with_simplified_config.execute_in_process(
        run_config={"simplified_param": "1!!!", "a": "2!!"}
    )
