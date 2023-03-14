package io.dexterity.util.securtiy.fast;

import io.dexterity.util.securtiy.Cipher;
import io.dexterity.util.securtiy.Key;
import io.dexterity.util.securtiy.OpeException;
import lombok.extern.slf4j.Slf4j;

import java.nio.ByteBuffer;
import java.security.SecureRandom;

/**
 * Cipher implementation of Hwang et al's Fast Order-Preserving Encryption scheme.
 * 
 * Reference:
 * 
 * Hwang, Y. H., Kim, S., & Seo, J. W. (2015, October).
 * Fast order-preserving encryption from uniform distribution sampling.
 * In Proceedings of the 2015 ACM Workshop on Cloud Computing Security Workshop (pp. 41-52). ACM.
 * https://dl.acm.org/citation.cfm?id=2808431
 * 
 * @author Ayman Madkour <info@aymanmadkour.com>
 */

@Slf4j
public class FastOpeCipher implements Cipher {
	
	private static final int DEFAULT_TAU = 16;
	
	private final SecureRandom rnd = new SecureRandom();
	
	private int tau = DEFAULT_TAU;
	
	public int getTau() {
		return tau;
	}
	
	public void setTau(int tau) {
		this.tau = (tau > 0) ? tau : DEFAULT_TAU;
	}

	public Key generateKey() throws OpeException {
		double alpha = rnd.nextDouble() / 2.0;
		double beta = 1.0 - alpha;
		double e = rnd.nextDouble() * alpha;
		long n = (long) Math.ceil((double) tau / (beta * Math.pow(e, 8)));
		long k = rnd.nextLong() & 0x7fffffffffffffffL;
		log.info("{},{},{},{}",alpha,e,k,n);

		return new FastOpeKey(n, alpha, e, k);
	}

	public Key generateKey(String s) throws OpeException {
		String[] split = s.split(",");
		double alpha = Double.parseDouble(split[0]);
		double beta = 1.0 - alpha;
		double e = Double.parseDouble(split[1]);
		long n = Long.parseLong(split[3]);
		long k = Long.parseLong(split[2]);
		log.info("{},{},{},{}",alpha,e,k,n);

		return new FastOpeKey(n, alpha, e, k);
	}


	public Key decodeKey(byte[] bytes) throws OpeException {
		ByteBuffer buffer = ByteBuffer.wrap(bytes);
		
		double alpha = buffer.getDouble() / 2.0;
		double e = buffer.getDouble() * alpha;
		long k = buffer.getLong() & 0x7fffffffffffffffL;
		long n = buffer.getLong();

		log.info("{},{},{},{}",alpha,e,k,n);
		
		return new FastOpeKey(n, alpha, e, k);
	}
}
