'use client'

import { AppShell } from '@/components/layout/AppShell'
import { S3SettingsForm } from './S3SettingsForm'
import { Button } from '@/components/ui/button'
import { RefreshCw, ArrowLeft } from 'lucide-react'
import { useS3Status, useS3Config } from '@/lib/hooks/use-s3-config'
import Link from 'next/link'

export default function S3SettingsPage() {
  const { refetch: refetchStatus } = useS3Status()
  const { refetch: refetchConfig } = useS3Config()

  const handleRefresh = () => {
    refetchStatus()
    refetchConfig()
  }

  return (
    <AppShell>
      <div className="flex-1 overflow-y-auto">
        <div className="p-6">
          <div className="max-w-4xl">
            <div className="flex items-center gap-4 mb-6">
              <Link href="/settings">
                <Button variant="ghost" size="sm">
                  <ArrowLeft className="h-4 w-4 mr-2" />
                  Back to Settings
                </Button>
              </Link>
            </div>
            <div className="flex items-center gap-4 mb-6">
              <h1 className="text-2xl font-bold">S3 Storage Configuration</h1>
              <Button variant="outline" size="sm" onClick={handleRefresh}>
                <RefreshCw className="h-4 w-4" />
              </Button>
            </div>
            <S3SettingsForm />
          </div>
        </div>
      </div>
    </AppShell>
  )
}
