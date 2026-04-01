use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregationType {
    Sum,
    Min,
    Max,
    CountDistinct,
    SumBoolean,
    Average,
    Percentile,
    Median,
    Count,
}

impl FromStr for AggregationType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "sum" => Ok(Self::Sum),
            "min" => Ok(Self::Min),
            "max" => Ok(Self::Max),
            "count_distinct" => Ok(Self::CountDistinct),
            "sum_boolean" => Ok(Self::SumBoolean),
            "average" => Ok(Self::Average),
            "percentile" => Ok(Self::Percentile),
            "median" => Ok(Self::Median),
            "count" => Ok(Self::Count),
            _ => Err(format!("unknown aggregation type: {s}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TimeGrain {
    Nanosecond,
    Microsecond,
    Millisecond,
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Quarter,
    Year,
}

impl FromStr for TimeGrain {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "nanosecond" => Ok(Self::Nanosecond),
            "microsecond" => Ok(Self::Microsecond),
            "millisecond" => Ok(Self::Millisecond),
            "second" => Ok(Self::Second),
            "minute" => Ok(Self::Minute),
            "hour" => Ok(Self::Hour),
            "day" => Ok(Self::Day),
            "week" => Ok(Self::Week),
            "month" => Ok(Self::Month),
            "quarter" => Ok(Self::Quarter),
            "year" => Ok(Self::Year),
            _ => Err(format!("unknown time grain: {s}")),
        }
    }
}

impl fmt::Display for TimeGrain {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Self::Nanosecond => "nanosecond",
            Self::Microsecond => "microsecond",
            Self::Millisecond => "millisecond",
            Self::Second => "second",
            Self::Minute => "minute",
            Self::Hour => "hour",
            Self::Day => "day",
            Self::Week => "week",
            Self::Month => "month",
            Self::Quarter => "quarter",
            Self::Year => "year",
        };
        write!(f, "{s}")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MetricKind {
    Simple,
    Derived,
    Cumulative,
    Conversion,
    Ratio,
}

impl FromStr for MetricKind {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "simple" => Ok(Self::Simple),
            "derived" => Ok(Self::Derived),
            "cumulative" => Ok(Self::Cumulative),
            "conversion" => Ok(Self::Conversion),
            "ratio" => Ok(Self::Ratio),
            _ => Err(format!("unknown metric kind: {s}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EntityType {
    Primary,
    Foreign,
    Natural,
    Unique,
}

impl FromStr for EntityType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "primary" => Ok(Self::Primary),
            "foreign" => Ok(Self::Foreign),
            "natural" => Ok(Self::Natural),
            "unique" => Ok(Self::Unique),
            _ => Err(format!("unknown entity type: {s}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DimensionType {
    Categorical,
    Time,
}

impl FromStr for DimensionType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "categorical" => Ok(Self::Categorical),
            "time" => Ok(Self::Time),
            _ => Err(format!("unknown dimension type: {s}")),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JoinType {
    LeftOuter,
    Inner,
    FullOuter,
    CrossJoin,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TimeWindow {
    pub count: u64,
    pub grain: TimeGrain,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregation_type_from_str() {
        assert_eq!("sum".parse::<AggregationType>().unwrap(), AggregationType::Sum);
        assert_eq!("count_distinct".parse::<AggregationType>().unwrap(), AggregationType::CountDistinct);
        assert_eq!("SUM".parse::<AggregationType>().unwrap(), AggregationType::Sum);
    }

    #[test]
    fn test_time_grain_from_str() {
        assert_eq!("day".parse::<TimeGrain>().unwrap(), TimeGrain::Day);
        assert_eq!("MONTH".parse::<TimeGrain>().unwrap(), TimeGrain::Month);
    }

    #[test]
    fn test_time_grain_ordering() {
        assert!(TimeGrain::Day < TimeGrain::Week);
        assert!(TimeGrain::Week < TimeGrain::Month);
        assert!(TimeGrain::Month < TimeGrain::Quarter);
        assert!(TimeGrain::Quarter < TimeGrain::Year);
    }

    #[test]
    fn test_metric_type_from_str() {
        assert_eq!("simple".parse::<MetricKind>().unwrap(), MetricKind::Simple);
        assert_eq!("derived".parse::<MetricKind>().unwrap(), MetricKind::Derived);
        assert_eq!("cumulative".parse::<MetricKind>().unwrap(), MetricKind::Cumulative);
        assert_eq!("conversion".parse::<MetricKind>().unwrap(), MetricKind::Conversion);
        assert_eq!("ratio".parse::<MetricKind>().unwrap(), MetricKind::Ratio);
    }

    #[test]
    fn test_entity_type_from_str() {
        assert_eq!("primary".parse::<EntityType>().unwrap(), EntityType::Primary);
        assert_eq!("foreign".parse::<EntityType>().unwrap(), EntityType::Foreign);
        assert_eq!("unique".parse::<EntityType>().unwrap(), EntityType::Unique);
        assert_eq!("natural".parse::<EntityType>().unwrap(), EntityType::Natural);
    }

    #[test]
    fn test_dimension_type_from_str() {
        assert_eq!("categorical".parse::<DimensionType>().unwrap(), DimensionType::Categorical);
        assert_eq!("time".parse::<DimensionType>().unwrap(), DimensionType::Time);
    }
}
