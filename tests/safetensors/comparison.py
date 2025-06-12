import torch


# Compare tensors Re-interpret the float tensors as integer types to compare their bit patterns.
# This avoids the "NaN != NaN" floating-point issue.
def are_tensors_bitwise_identical(name, t1, t2):
    if t1.dtype != t2.dtype:
        return False,  f"Tensor {name} has different dtype: {t1.dtype} vs {t2.dtype}"
    if t1.shape != t2.shape:
        return False, f"Tensor {name} has different shape: {t1.shape} vs {t2.shape}"
    
    # We must select the correct integer size based on the float dtype.
    if t1.dtype == torch.float32:
        t1 = t1.view(torch.int32)
        t2 = t2.view(torch.int32)
    elif t1.dtype == torch.float16:
        t1 = t1.view(torch.int16)
        t2 = t2.view(torch.int16)
    elif t1.dtype == torch.float64:
        t1 = t1.view(torch.int64)
        t2 = t2.view(torch.int64)
    elif t1.dtype == torch.bfloat16:
        t1 = t1.view(torch.int16)
        t2 = t2.view(torch.int16)

    res = torch.equal(t1, t2)
    if res is False:
        return False, f"Tensor {name} has different values"
    return True, f"Tensor {name} is bitwise identical"

def tensor_maps_are_equal(first, second):
    if len(first.items()) != len(second.items()):
        return False, "Does not have the same number of tensors"
    
    for name, our_tensor in first.items():
        if not our_tensor.is_contiguous():
            return False, f"Tensor {name} is not contiguous"
        
        eq, err = are_tensors_bitwise_identical(name, our_tensor, second[name])
        if not eq:
            return False, err
    return True, "Tensors match successfully"