import * as glue from '@aws-cdk/aws-glue-alpha';

export const columns = [
    {
        name: 'function_name',
        type: glue.Schema.STRING,
    }, {
        name: 'log_group_name',
        type: glue.Schema.STRING,
    }, {
        name: 'architecture',
        type: glue.Schema.STRING,
    }, {
        name: 'provisioned_memory_mb',
        type: glue.Schema.INTEGER,
    }, {
        name: 'min_memory_used_mb',
        type: glue.Schema.INTEGER,
    }, {
        name: 'avg_memory_used_mb',
        type: glue.Schema.FLOAT,
    }, {
        name: 'max_memory_used_mb',
        type: glue.Schema.INTEGER,
    }, {
        name: 'over_provisioned_memory_mb',
        type: glue.Schema.INTEGER,
    }, {
        name: 'avg_billed_duration_ms',
        type: glue.Schema.FLOAT,
    }, {
        name: 'invocations',
        type: glue.Schema.BIG_INT,
    }, {
        name: 'potential_savings',
        type: glue.Schema.DOUBLE,
    },
];

export const partitions = [
    {
        name: 'metric_collection_date',
        type: glue.Schema.DATE,
    },
];
