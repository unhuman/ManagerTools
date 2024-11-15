package com.unhuman.flexidb.output

import com.unhuman.flexidb.FlexiDB;

class ConvertZerosToEmptyOutputFilter implements OutputFilter {
    @Override
    Object apply(String columnName, Object value) {
        return (value == 0) ? FlexiDB.EMPTY_INCREMENTOR : value;
    }
}
