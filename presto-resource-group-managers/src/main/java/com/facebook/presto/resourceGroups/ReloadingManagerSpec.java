package com.facebook.presto.resourceGroups;

import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static java.util.Objects.requireNonNull;

public class ReloadingManagerSpec
{
    private final ManagerSpec managerSpec;
    private final Map<ResourceGroupIdTemplate, ResourceGroupSpec> resourceGroups;

    public ReloadingManagerSpec(ManagerSpec managerSpec, Map<ResourceGroupIdTemplate, ResourceGroupSpec> resourceGroups)
    {
        this.managerSpec = requireNonNull(managerSpec, "managerSpec is null");
        this.resourceGroups = ImmutableMap.copyOf(requireNonNull(resourceGroups, "resourceGroups is null"));
    }

    public ManagerSpec getManagerSpec()
    {
        return managerSpec;
    }

    public Map<ResourceGroupIdTemplate, ResourceGroupSpec> getResourceGroups()
    {
        return resourceGroups;
    }
}
