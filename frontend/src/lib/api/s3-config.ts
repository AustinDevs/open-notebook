import { apiClient } from './client'

export interface S3ConfigStatus {
  configured: boolean
  source: 'database' | 'environment' | 'none'
  bucket_name?: string
  region?: string
  endpoint_url?: string
}

export interface S3Config {
  bucket_name?: string
  region?: string
  endpoint_url?: string
  public_url?: string
  use_path_style: boolean
  has_credentials: boolean
}

export interface S3ConfigRequest {
  access_key_id: string
  secret_access_key: string
  bucket_name: string
  region?: string
  endpoint_url?: string
  public_url?: string
  use_path_style?: boolean
}

export interface S3TestResult {
  success: boolean
  message: string
}

export const s3ConfigApi = {
  /**
   * Get S3 configuration status
   */
  getStatus: async (): Promise<S3ConfigStatus> => {
    const response = await apiClient.get<S3ConfigStatus>('/s3-config/status')
    return response.data
  },

  /**
   * Get current S3 configuration (no secrets returned)
   */
  getConfig: async (): Promise<S3Config> => {
    const response = await apiClient.get<S3Config>('/s3-config')
    return response.data
  },

  /**
   * Save S3 configuration
   */
  saveConfig: async (config: S3ConfigRequest): Promise<S3Config> => {
    const response = await apiClient.post<S3Config>('/s3-config', config)
    return response.data
  },

  /**
   * Delete S3 configuration
   */
  deleteConfig: async (): Promise<void> => {
    await apiClient.delete('/s3-config')
  },

  /**
   * Test S3 connection
   */
  testConnection: async (): Promise<S3TestResult> => {
    const response = await apiClient.post<S3TestResult>('/s3-config/test')
    return response.data
  },
}
