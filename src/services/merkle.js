/**
 * Merkle Tree — SHA-256 via crypto.subtle
 * Computes roots, generates inclusion proofs, verifies proofs.
 */

async function sha256(str) {
  const encoder = new TextEncoder();
  const data = encoder.encode(str);
  const hashBuffer = await crypto.subtle.digest('SHA-256', data);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

/**
 * Compute Merkle root from an array of fingerprints.
 */
export async function computeMerkleRoot(fingerprints) {
  if (fingerprints.length === 0) return null;
  if (fingerprints.length === 1) return await sha256(fingerprints[0]);

  let leaves = [];
  for (const fp of fingerprints) {
    leaves.push(await sha256(fp));
  }

  if (leaves.length % 2 !== 0) {
    leaves.push(leaves[leaves.length - 1]);
  }

  while (leaves.length > 1) {
    const next = [];
    for (let i = 0; i < leaves.length; i += 2) {
      next.push(await sha256(leaves[i] + leaves[i + 1]));
    }
    leaves = next;
    if (leaves.length > 1 && leaves.length % 2 !== 0) {
      leaves.push(leaves[leaves.length - 1]);
    }
  }

  return leaves[0];
}

/**
 * Generate a Merkle inclusion proof for a leaf at the given index.
 * Returns an array of { hash, position } objects ('left' or 'right').
 */
export async function generateProof(fingerprints, targetIndex) {
  if (fingerprints.length === 0 || targetIndex >= fingerprints.length) return null;

  let leaves = [];
  for (const fp of fingerprints) {
    leaves.push(await sha256(fp));
  }

  if (leaves.length % 2 !== 0) {
    leaves.push(leaves[leaves.length - 1]);
  }

  const proof = [];
  let idx = targetIndex;

  while (leaves.length > 1) {
    if (idx % 2 === 0) {
      proof.push({ hash: leaves[idx + 1], position: 'right' });
    } else {
      proof.push({ hash: leaves[idx - 1], position: 'left' });
    }

    const next = [];
    for (let i = 0; i < leaves.length; i += 2) {
      next.push(await sha256(leaves[i] + leaves[i + 1]));
    }
    leaves = next;
    idx = Math.floor(idx / 2);

    if (leaves.length > 1 && leaves.length % 2 !== 0) {
      leaves.push(leaves[leaves.length - 1]);
    }
  }

  return proof;
}

/**
 * Verify a Merkle proof: leaf + proof steps → should equal expectedRoot.
 */
export async function verifyProof(leafFingerprint, proof, expectedRoot) {
  let current = await sha256(leafFingerprint);

  for (const step of proof) {
    if (step.position === 'left') {
      current = await sha256(step.hash + current);
    } else {
      current = await sha256(current + step.hash);
    }
  }

  return current === expectedRoot;
}

export { sha256 };
