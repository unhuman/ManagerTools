package com.unhuman.flexidb.output

import com.unhuman.flexidb.FlexiDB;

class ConvertEmptyToZeroOutputFilter implements OutputFilter {
    @Override
    Object apply(String columnName, Object value) {
        return (value == FlexiDB.EMPTY_INCREMENTOR) ? 0 : value;
    }
}
