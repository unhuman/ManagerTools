package com.unhuman.managertools.output

import com.unhuman.flexidb.output.ConvertEmptyToZeroOutputFilter
import com.unhuman.managertools.data.UserActivity

class ConvertSelfMetricsEmptyToZeroOutputFilter extends ConvertEmptyToZeroOutputFilter {
    @Override
    Object apply(String columnName, Object value) {
        return (columnName == UserActivity.OTHERS_COMMENTED.name())
            ? super.apply(columnName, value) : value;
    }
}
