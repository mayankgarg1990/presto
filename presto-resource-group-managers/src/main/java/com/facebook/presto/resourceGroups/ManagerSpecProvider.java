package com.facebook.presto.resourceGroups;

import java.util.List;

public interface ManagerSpecProvider
{
    ReloadingManagerSpec getSpec();

    List<ResourceGroupSelector> getExactMatchSelectors();
}
