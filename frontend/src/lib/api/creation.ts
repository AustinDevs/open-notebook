import apiClient from './client'
import { insightsApi } from './insights'
import {
  CreationArtifact,
  CreatorsResponse,
  GenerateCreationRequest,
  GenerateCreationResponse,
} from '@/lib/types/creation'

export const creationApi = {
  listCreators: async () => {
    const response = await apiClient.get<CreatorsResponse>('/creation/creators')
    return response.data
  },

  listArtifacts: async (params: { creator_key?: string; notebook_id?: string }) => {
    const response = await apiClient.get<CreationArtifact[]>('/creation/artifacts', {
      params,
    })
    return response.data
  },

  getArtifact: async (id: string) => {
    const response = await apiClient.get<CreationArtifact>(`/creation/artifacts/${id}`)
    return response.data
  },

  generate: async (payload: GenerateCreationRequest) => {
    const response = await apiClient.post<GenerateCreationResponse>(
      '/creation/artifacts/generate',
      payload
    )
    return response.data
  },

  deleteArtifact: async (id: string) => {
    await apiClient.delete(`/creation/artifacts/${id}`)
  },

  downloadFile: async (artifactId: string, fileIndex: number): Promise<Blob> => {
    const response = await apiClient.get<Blob>(
      `/creation/artifacts/${artifactId}/files/${fileIndex}`,
      { responseType: 'blob' }
    )
    return response.data
  },

  // Fetch a creator's self-contained HTML view bundle. Fetched (not iframe-src'd)
  // so the auth header is sent; the caller object-URLs it into a sandboxed iframe.
  getCreatorView: async (creatorKey: string): Promise<Blob> => {
    const response = await apiClient.get<Blob>(
      `/creation/creators/${creatorKey}/view`,
      { responseType: 'blob' }
    )
    return response.data
  },

  // Reuse the generic surreal-commands job poller.
  waitForCommand: insightsApi.waitForCommand,
}

export function triggerBrowserDownload(blob: Blob, filename: string) {
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  document.body.appendChild(a)
  a.click()
  a.remove()
  URL.revokeObjectURL(url)
}
